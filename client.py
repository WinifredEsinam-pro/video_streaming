import tkinter as tk
import socket
import threading
import re
import subprocess
import tempfile
import os
import queue
import numpy as np
from PIL import Image, ImageTk

try:
    import signal as _signal
except ImportError:
    _signal = None


RTSP_PORT      = 8554
RTP_PORT       = 5004
AUDIO_RTP_PORT = 5020
CRLF           = "\r\n"


rtsp_socket       = None
session_id        = None
rtsp_host         = "127.0.0.1"
rtsp_path         = "stream.mp4"
rtsp_full_url     = ""
rtsp_state        = "DISCONNECTED"

video_width       = 0
video_height      = 0
has_audio         = False
audio_port        = AUDIO_RTP_PORT

ffmpeg_video_proc = None
ffplay_proc       = None
video_thread      = None
video_stop_event  = threading.Event()
_audio_sdp_file   = None
_frame_queue      = queue.Queue(maxsize=1)
_cseq             = 1
_poll_generation  = 0
_rtsp_lock        = threading.Lock()
_audio_start_generation = -1


def update_status(text, color="black"):
    connection_status.config(text=f"Status: {text}", fg=color)

def _recv_full_response(sock):
    data = b""
    while (CRLF + CRLF).encode() not in data:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            return None
        if not chunk:
            break
        data += chunk
    text = data.decode("utf-8", errors="ignore")
    m = re.search(r"Content-Length:\s*(\d+)", text, re.IGNORECASE)
    if m:
        need = int(m.group(1))
        have = len(data) - data.index(b"\r\n\r\n") - 4
        remaining = need - have
        while remaining > 0:
            chunk = sock.recv(min(4096, remaining))
            if not chunk:
                break
            data      += chunk
            remaining -= len(chunk)
    return data.decode("utf-8", errors="ignore")

def _send_rtsp(method, extra_headers=None):
    global session_id, _cseq, rtsp_socket
    with _rtsp_lock:
        if rtsp_socket is None:
            return None
        url = rtsp_full_url
        lines = [
            f"{method} {url} RTSP/1.0",
            f"CSeq: {_cseq}",
        ]
        if method == "SETUP":
            lines.append(f"Transport: RTP/UDP;unicast;client_port={RTP_PORT}-{RTP_PORT+1}")
        elif session_id:
            lines.append(f"Session: {session_id}")
        if extra_headers:
            for k, v in extra_headers.items():
                lines.append(f"{k}: {v}")
        request = CRLF.join(lines) + CRLF + CRLF
        print(f"\n[RTSP >>]\n{request.strip()}")
        try:
            rtsp_socket.sendall(request.encode("utf-8"))
            resp = _recv_full_response(rtsp_socket)
            print(f"[RTSP <<]\n{resp}")
            if resp and "Session:" in resp:
                for line in resp.splitlines():
                    if line.startswith("Session:"):
                        session_id = line.split(":", 1)[1].strip().split(";")[0]
                        break
            _cseq += 1
            return resp
        except Exception as e:
            print(f"[RTSP] {method} error: {e}")
            return None


def _parse_sdp(sdp_body):
    global video_width, video_height, has_audio, audio_port
    for line in sdp_body.splitlines():
        line = line.strip()
        if line.startswith("a=x-dimensions:"):
            try:
                w, h = line.split(":")[1].split(",")
                video_width, video_height = int(w), int(h)
            except Exception:
                pass
        if line.startswith("m=audio"):
            has_audio = True
            try:
                audio_port = int(line.split()[1])
            except Exception:
                pass


def _log_stderr(proc, label):
    for line in proc.stderr:
        print(f"[{label}] {line.decode(errors='ignore').rstrip()}")

def _ffmpeg_video_reader(gen):
    global ffmpeg_video_proc, video_width, video_height

    w, h = video_width or 1280, video_height or 720

    sdp = (
        "v=0\r\n"
        f"o=- 0 0 IN IP4 {rtsp_host}\r\n"
        "s=Video\r\n"
        f"c=IN IP4 {rtsp_host}\r\n"
        "t=0 0\r\n"
        f"m=video {RTP_PORT} RTP/AVP 96\r\n"
        "a=rtpmap:96 H264/90000\r\n"
    )
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".sdp", delete=False)
    tmp.write(sdp)
    tmp.close()
    sdp_path = tmp.name

    cmd = [
        "ffmpeg",
        "-loglevel",           "warning",
        "-protocol_whitelist", "file,udp,rtp",
        "-fflags",             "nobuffer",
        "-flags",              "low_delay",
        "-i",                  sdp_path,
        "-f",                  "rawvideo",
        "-pix_fmt",            "bgr24",
        "-vf",                 f"scale={w}:{h}",
        "pipe:1",
    ]
    print(f"[Video] {' '.join(cmd)}")

    kw = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
    if os.name == "nt":
        kw["creationflags"] = 0x08000000
    elif _signal:
        kw["preexec_fn"] = os.setsid

    ffmpeg_video_proc = subprocess.Popen(cmd, **kw)
    threading.Thread(target=_log_stderr, args=(ffmpeg_video_proc, "Video"), daemon=True).start()

    frame_size = w * h * 3
    first_frame = True
    while not video_stop_event.is_set():
        raw = ffmpeg_video_proc.stdout.read(frame_size)
        if len(raw) < frame_size:
            break
        frame = np.frombuffer(raw, dtype=np.uint8).reshape((h, w, 3))
        try:
            _frame_queue.get_nowait()
        except queue.Empty:
            pass
        try:
            _frame_queue.put_nowait(frame)
        except queue.Full:
            pass
        if first_frame:
            first_frame = False
            root.after(0, _trigger_audio_start, gen)

    try:
        os.remove(sdp_path)
    except Exception:
        pass

def _poll_frame(gen=0):
    if gen != _poll_generation or video_stop_event.is_set():
        return
    try:
        frame     = _frame_queue.get_nowait()
        frame_rgb = frame[:, :, ::-1]
        pil_img   = Image.fromarray(frame_rgb)
        vw = video_frame.winfo_width()  or 1000
        vh = video_frame.winfo_height() or 500
        scale   = min(vw / pil_img.width, vh / pil_img.height, 1.0)
        new_w   = max(1, int(pil_img.width  * scale))
        new_h   = max(1, int(pil_img.height * scale))
        pil_img = pil_img.resize((new_w, new_h), Image.LANCZOS)
        photo   = ImageTk.PhotoImage(pil_img)
        video_label.config(image=photo, text="")
        video_label.image = photo
    except queue.Empty:
        pass
    root.after(33, _poll_frame, gen)

def _trigger_audio_start(gen):
    global _audio_start_generation
    if gen != _poll_generation or video_stop_event.is_set():
        return
    if _audio_start_generation == gen:
        return
    _audio_start_generation = gen
    start_audio()

def start_video():
    global video_thread, _poll_generation
    _poll_generation += 1
    gen = _poll_generation
    video_stop_event.clear()
    while not _frame_queue.empty():
        try:
            _frame_queue.get_nowait()
        except queue.Empty:
            break
    video_thread = threading.Thread(target=_ffmpeg_video_reader, args=(gen,), daemon=True)
    video_thread.start()
    root.after(33, _poll_frame, gen)

def stop_video():
    global ffmpeg_video_proc
    video_stop_event.set()
    if ffmpeg_video_proc and ffmpeg_video_proc.poll() is None:
        try:
            if os.name == "nt":
                ffmpeg_video_proc.terminate()
            elif _signal:
                os.killpg(os.getpgid(ffmpeg_video_proc.pid), _signal.SIGTERM)
        except Exception as e:
            print(f"[Video] stop error: {e}")
        try:
            ffmpeg_video_proc.wait(timeout=3)
        except Exception:
            ffmpeg_video_proc.kill()
        ffmpeg_video_proc = None


def start_audio():
    global ffplay_proc, _audio_sdp_file
    stop_audio()
    if not has_audio:
        print("[Audio] No audio track.")
        return

    sdp = (
        "v=0\r\n"
        f"o=- 0 0 IN IP4 {rtsp_host}\r\n"
        "s=Audio\r\n"
        f"c=IN IP4 {rtsp_host}\r\n"
        "t=0 0\r\n"
        f"m=audio {audio_port} RTP/AVP 14\r\n"
        "b=AS:128\r\n"
        "a=rtpmap:14 MPA/90000\r\n"
    )
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".sdp", delete=False)
    tmp.write(sdp)
    tmp.close()
    _audio_sdp_file = tmp.name
    print(f"[Audio] SDP → {_audio_sdp_file}")

    cmd = [
        "ffplay",
        "-loglevel",           "warning",
        "-protocol_whitelist", "file,udp,rtp",
        "-i",                  _audio_sdp_file,
        "-nodisp",
        "-vn",
        "-infbuf",
        "-sync",               "ext",
        "-af",                 "aresample=async=1",
    ]
    print(f"[Audio] {' '.join(cmd)}")

    kw = {"stdout": subprocess.DEVNULL, "stderr": subprocess.PIPE}
    if os.name == "nt":
        kw["creationflags"] = 0x08000000

    try:
        ffplay_proc = subprocess.Popen(cmd, **kw)
        threading.Thread(target=_log_stderr, args=(ffplay_proc, "Audio"), daemon=True).start()
        print(f"[Audio] ffplay PID {ffplay_proc.pid}")
    except FileNotFoundError:
        print("[Audio] ffplay not found — install ffmpeg and add to PATH")
    except Exception as e:
        print(f"[Audio] error: {e}")

def stop_audio():
    global ffplay_proc, _audio_sdp_file
    if ffplay_proc and ffplay_proc.poll() is None:
        try:
            ffplay_proc.terminate()
            ffplay_proc.wait(timeout=2)
        except Exception:
            try:
                ffplay_proc.kill()
                ffplay_proc.wait(timeout=2)
            except Exception:
                pass
    ffplay_proc = None
    if _audio_sdp_file and os.path.exists(_audio_sdp_file):
        try:
            os.remove(_audio_sdp_file)
        except Exception:
            pass
    _audio_sdp_file = None


def setup():
    global rtsp_socket, rtsp_host, rtsp_path, rtsp_state, rtsp_full_url
    global session_id, _cseq, video_width, video_height, has_audio, audio_port

    url = entry.get().strip()
    if not url:
        update_status("Please enter a stream URL", "red")
        return

    rtsp_full_url = url
    no_proto = url.replace("rtsp://", "")
    rtsp_host = no_proto.split("/")[0].split(":")[0]

    session_id   = None
    _cseq        = 1
    has_audio    = False
    video_width  = 0
    video_height = 0

    def _do():
        global rtsp_socket, rtsp_state
        try:
            with _rtsp_lock:
                if rtsp_socket:
                    try:
                        rtsp_socket.close()
                    except Exception:
                        pass
                rtsp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                rtsp_socket.settimeout(5.0)
                rtsp_socket.connect((rtsp_host, RTSP_PORT))
            root.after(0, update_status, "Connected — negotiating…", "orange")

            resp = _send_rtsp("OPTIONS")
            if not resp or len(resp.split()) < 2 or "200" not in resp.split()[1]:
                raise Exception("OPTIONS failed")

            resp = _send_rtsp("DESCRIBE", {"Accept": "application/sdp"})
            if not resp or len(resp.split()) < 2 or "200" not in resp.split()[1]:
                raise Exception("DESCRIBE failed")
            if CRLF + CRLF in resp:
                _parse_sdp(resp.split(CRLF + CRLF, 1)[1])
            print(f"[Client] {video_width}x{video_height}  audio={has_audio} port={audio_port}")

            resp = _send_rtsp("SETUP")
            if not resp or len(resp.split()) < 2 or "200" not in resp.split()[1]:
                raise Exception("SETUP failed")

            rtsp_state = "READY"
            root.after(0, update_status, "Ready — click Play", "green")
            root.after(0, lambda: video_label.config(text="Ready — click Play", image=""))

        except Exception as e:
            root.after(0, update_status, f"Setup failed: {e}", "red")
            with _rtsp_lock:
                if rtsp_socket:
                    try:
                        rtsp_socket.close()
                    except Exception:
                        pass
                rtsp_socket = None
            rtsp_state = "DISCONNECTED"

    threading.Thread(target=_do, daemon=True).start()


def play():
    global rtsp_state

    if rtsp_socket is None or rtsp_state != "READY":
        update_status("Run Setup first", "red")
        return

    def _do():
        global rtsp_state
        resp = _send_rtsp("PLAY")
        if resp and len(resp.split()) >= 2 and "200" in resp.split()[1]:
            rtsp_state = "PLAYING"
            root.after(0, update_status, "Playing", "green")
            root.after(0, start_video)
        else:
            root.after(0, update_status, "PLAY failed", "red")

    threading.Thread(target=_do, daemon=True).start()


def pause():
    global rtsp_state

    if rtsp_socket is None or rtsp_state != "PLAYING":
        update_status("Not playing", "red")
        return

    def _do():
        global rtsp_state
        resp = _send_rtsp("PAUSE")
        if resp and len(resp.split()) >= 2 and "200" in resp.split()[1]:
            rtsp_state = "READY"
            stop_audio()
            stop_video()
            root.after(0, update_status, "Paused", "orange")
            root.after(0, lambda: video_label.config(text="Paused — click Play to resume", image=""))
        else:
            root.after(0, update_status, "PAUSE failed", "red")

    threading.Thread(target=_do, daemon=True).start()


def teardown():
    global rtsp_socket, session_id, rtsp_state

    def _do():
        global rtsp_socket, session_id, rtsp_state
        _send_rtsp("TEARDOWN")
        stop_audio()
        stop_video()
        with _rtsp_lock:
            if rtsp_socket:
                try:
                    rtsp_socket.close()
                except Exception:
                    pass
            rtsp_socket = None
            session_id  = None
        rtsp_state  = "DISCONNECTED"
        root.after(0, update_status, "Disconnected", "red")
        root.after(0, lambda: video_label.config(text="Video Stream...", image=""))

    threading.Thread(target=_do, daemon=True).start()


root = tk.Tk()
root.title("Group 5")
root.geometry("1280x720")

top_frame = tk.Frame(root)
top_frame.pack(pady=10)

tk.Label(top_frame, text="Stream URL:").pack(side="left", padx=5)

entry = tk.Entry(top_frame, width=50)
entry.pack(side="left", padx=5)
entry.insert(0, "rtsp://127.0.0.1/Download.mp4")

connection_status = tk.Label(top_frame, text="Status: Disconnected",
                              fg="red", font=("Arial", 11))
connection_status.pack(side="left", padx=10)

video_frame = tk.Frame(root, width=1000, height=500,
                        bg="black", relief="solid", bd=2)
video_frame.pack(pady=20)
video_frame.pack_propagate(False)

video_label = tk.Label(video_frame, text="Video Stream...",
                        bg="black", fg="white", font=("Arial", 15))
video_label.pack(expand=True)

bottom_frame = tk.Frame(root)
bottom_frame.pack(pady=10)

tk.Button(bottom_frame, text="Setup",    command=setup)   .pack(side="left", padx=5)
tk.Button(bottom_frame, text="Play",     command=play)    .pack(side="left", padx=5)
tk.Button(bottom_frame, text="Pause",    command=pause)   .pack(side="left", padx=5)
tk.Button(bottom_frame, text="Teardown", command=teardown).pack(side="left", padx=5)


def _on_close():
    video_stop_event.set()
    stop_audio()
    stop_video()
    if rtsp_socket is not None:
        try:
            _send_rtsp("TEARDOWN")
        except Exception:
            pass
        try:
            rtsp_socket.close()
        except Exception:
            pass
    root.destroy()


root.protocol("WM_DELETE_WINDOW", _on_close)
root.mainloop()