import socket
import threading
import random
import subprocess
import os
import signal
import platform

RTSP_PORT         = 8554
CRLF              = "\r\n"
AUDIO_PORT_OFFSET = 16

STATE_INIT    = 0
STATE_READY   = 1
STATE_PLAYING = 2


def get_camera_input():
    """
    Returns the ffmpeg -f and -i arguments for the webcam
    depending on the operating system.
    """
    system = platform.system()
    if system == "Windows":
        return ["-f", "dshow",      "-i", "video=Integrated Webcam"]
    elif system == "Darwin":        # macOS
        return ["-f", "avfoundation", "-i", "0"]
    else:                           # Linux
        return ["-f", "v4l2",       "-i", "/dev/video0"]


class RTSPServerWorker(threading.Thread):
    def __init__(self, client_socket, client_address):
        super().__init__(daemon=True)
        self.client_socket  = client_socket
        self.client_address = client_address
        self.state          = STATE_INIT
        self.session_id     = str(random.randint(100000, 999999))
        self.client_rtp_port = None
        self.ffmpeg_proc    = None
        self.cseq           = 1

    def run(self):
        print(f"[RTSP] Client connected: {self.client_address}")
        self.client_socket.settimeout(30.0)
        try:
            buf = ""
            while True:
                try:
                    chunk = self.client_socket.recv(4096).decode("utf-8", errors="ignore")
                except socket.timeout:
                    continue
                if not chunk:
                    break
                buf += chunk
                while CRLF + CRLF in buf:
                    msg, buf = buf.split(CRLF + CRLF, 1)
                    print(f"\n[RTSP] >> Request:\n{msg}\n")
                    self.handle_rtsp_request(msg)
        except (ConnectionResetError, OSError):
            pass
        finally:
            self._stop_ffmpeg()
            self.client_socket.close()
            print(f"[RTSP] Client {self.client_address} disconnected.")

    def handle_rtsp_request(self, message):
        lines  = message.split(CRLF)
        if not lines:
            return
        parts  = lines[0].split()
        if len(parts) < 2:
            return
        method = parts[0]

        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        cseq = headers.get("cseq", str(self.cseq))

        dispatch = {
            "OPTIONS":  self.handle_options,
            "DESCRIBE": self.handle_describe,
            "SETUP":    self.handle_setup,
            "PLAY":     self.handle_play,
            "PAUSE":    self.handle_pause,
            "TEARDOWN": self.handle_teardown,
        }
        handler = dispatch.get(method)
        if handler:
            handler(parts, headers, cseq)
        else:
            self.send_response(501, cseq)

    def handle_options(self, parts, headers, cseq):
        self.send_response(200, cseq, {
            "Public": "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN"
        })

    def handle_describe(self, parts, headers, cseq):
        # Live camera — no file to check, always available
        # We use fixed 640x480 for webcam compatibility
        sdp = self._build_sdp(width=640, height=480)
        self.send_response(200, cseq, {
            "Content-Type":   "application/sdp",
            "Content-Length": str(len(sdp)),
        }, body=sdp)

    def handle_setup(self, parts, headers, cseq):
        if self.state not in (STATE_INIT, STATE_READY):
            self.send_response(455, cseq, {"Session": self.session_id})
            return

        transport        = headers.get("transport", "")
        client_port      = self._parse_client_port(transport)
        self.client_rtp_port = client_port
        self.state       = STATE_READY

        print(f"[RTSP] SETUP: client_rtp_port={client_port}")
        self.send_response(200, cseq, {
            "Session":   self.session_id,
            "Transport": (
                f"RTP/UDP;unicast;"
                f"client_port={client_port}-{client_port+1};"
                f"server_port={client_port+1}-{client_port+2}"
            ),
        })

    def handle_play(self, parts, headers, cseq):
        if self.state != STATE_READY:
            self.send_response(455, cseq, {"Session": self.session_id})
            return
        self.state = STATE_PLAYING
        self.send_response(200, cseq, {
            "Session":  self.session_id,
            "RTP-Info": f"url=rtsp://{self.client_address[0]}/live;seq=1;rtptime=0",
        })
        self._start_ffmpeg()

    def handle_pause(self, parts, headers, cseq):
        if self.state != STATE_PLAYING:
            self.send_response(455, cseq, {"Session": self.session_id})
            return
        self._stop_ffmpeg()
        self.state = STATE_READY
        self.send_response(200, cseq, {"Session": self.session_id})

    def handle_teardown(self, parts, headers, cseq):
        self._stop_ffmpeg()
        self.state = STATE_INIT
        self.send_response(200, cseq, {"Session": self.session_id})

    def _start_ffmpeg(self):
        if self.ffmpeg_proc and self.ffmpeg_proc.poll() is None:
            return

        client_ip  = self.client_address[0]
        video_port = self.client_rtp_port or 5004

        # Get the OS-specific webcam capture arguments
        cam_args = get_camera_input()

        cmd = (
            ["ffmpeg"]
            + cam_args                  # webcam input (OS-specific)
            + [
                "-vcodec",  "libx264",
                "-preset",  "ultrafast",
                "-tune",    "zerolatency",
                "-s",       "640x480",   # resolution
                "-r",       "30",        # 30 frames per second
                "-b:v",     "800k",
                "-maxrate", "900k",
                "-bufsize", "1600k",
                "-g",       "60",
                "-f",       "rtp",
                f"rtp://{client_ip}:{video_port}",
            ]
        )

        print(f"[FFmpeg] Starting: {' '.join(cmd)}")

        kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.PIPE}
        if os.name != "nt":
            kwargs["preexec_fn"] = os.setsid
        else:
            kwargs["creationflags"] = 0x08000000

        self.ffmpeg_proc = subprocess.Popen(cmd, **kwargs)
        threading.Thread(target=self._log_ffmpeg, daemon=True).start()

    def _stop_ffmpeg(self):
        if self.ffmpeg_proc and self.ffmpeg_proc.poll() is None:
            try:
                if os.name == "nt":
                    self.ffmpeg_proc.terminate()
                else:
                    os.killpg(os.getpgid(self.ffmpeg_proc.pid), signal.SIGTERM)
            except Exception as e:
                print(f"[FFmpeg] Stop error: {e}")
            try:
                self.ffmpeg_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.ffmpeg_proc.kill()
            self.ffmpeg_proc = None
            print("[FFmpeg] Stopped.")

    def _log_ffmpeg(self):
        for line in self.ffmpeg_proc.stderr:
            print(f"[FFmpeg] {line.decode(errors='ignore').rstrip()}")

    def _build_sdp(self, width=640, height=480):
        ip         = self.client_address[0]
        video_port = self.client_rtp_port or 5004
        return (
            "v=0\r\n"
            f"o=- 0 0 IN IP4 {ip}\r\n"
            "s=Live Stream\r\n"
            f"c=IN IP4 {ip}\r\n"
            "t=0 0\r\n"
            f"a=x-dimensions:{width},{height}\r\n"
            f"m=video {video_port} RTP/AVP 96\r\n"
            "a=rtpmap:96 H264/90000\r\n"
            "a=control:track1\r\n"
        )

    def _parse_client_port(self, transport):
        for part in transport.split(";"):
            if "client_port" in part:
                try:
                    return int(part.split("=")[1].split("-")[0])
                except Exception:
                    pass
        return 5004

    def send_response(self, code, cseq, headers=None, body=""):
        reasons = {
            200: "OK", 404: "Not Found",
            455: "Method Not Valid in This State", 501: "Not Implemented",
        }
        lines = [f"RTSP/1.0 {code} {reasons.get(code, 'Unknown')}", f"CSeq: {cseq}"]
        if headers:
            for k, v in headers.items():
                lines.append(f"{k}: {v}")
        if body:
            lines.append(f"Content-Length: {len(body)}")
        response = CRLF.join(lines) + CRLF + CRLF + body
        print(f"[RTSP] << Response:\n{response.strip()}\n")
        try:
            self.client_socket.sendall(response.encode("utf-8"))
        except OSError as e:
            print(f"[RTSP] Send error: {e}")


class RtspServer:
    def __init__(self, host="", port=RTSP_PORT):
        self.host = host
        self.port = port

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(10)
            print(f"[RTSP] Live Camera Server on port {self.port}")
            print(f"       Connect from client: rtsp://<THIS_PC_IP>/live\n")
            try:
                while True:
                    client_sock, client_addr = srv.accept()
                    RTSPServerWorker(client_sock, client_addr).start()
            except KeyboardInterrupt:
                print("\n[RTSP] Server shutting down.")


if __name__ == "__main__":
    RtspServer().start()