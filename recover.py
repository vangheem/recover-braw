import io, json
import os
import time

# Source disk to read from
# SOURCE_DISK = "/dev/sdb"
SOURCE_DISK = "/Users/nathan/Downloads/A001_11171838_C008.braw"
# SOURCE_DISK = "/dev/disk5"
# Target location to save recovered files to
# TARGET_LOCATION = "/Volumes/backup/braw-files"
TARGET_LOCATION = "."
# if failed, restart from where you left off
RESTART_FROM_POSITION = False


def human_bytes(num: int) -> str:
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "%3.1f%s" % (num, unit)
        num /= 1024.0
    return "%.1f%s" % (num, "Y")


def human_time(seconds: float) -> str:
    for unit in ["s", "m", "h", "d"]:
        if seconds < 60:
            return f"{seconds:.2f}{unit}"
        seconds /= 60.0
    return f"{seconds:.2f}d"


class DiskReader:
    chunk_size = 1024 * 1024 * 5

    def __init__(self, disk: io.BytesIO):
        self.disk = disk
        self.total_bytes_read = 0

    def __repr__(self) -> str:
        return f"<DiskReader {human_bytes(self.total_bytes_read)}>"

    def read_all(self) -> bytes:
        while True:
            byte = self.disk.read(1)
            if len(byte) == 0:
                return
            self.total_bytes_read += 1
            yield byte

    def seek(self, target: int) -> None:
        self.total_bytes_read = target
        self.disk.seek(target)

    def peek(self, size: int) -> bytes:
        pos = self.disk.tell()
        val = self.disk.read(size)
        self.disk.seek(pos)
        return val

    def read(self, size: int) -> bytes:
        val = self.disk.read(size)
        if len(val) != size:
            print(f"Expected {size} bytes, got {len(val)}")
        self.total_bytes_read += size
        return val


class FileWriter:
    def __init__(self, filename: str):
        self.filename = filename
        self.fi = open(os.path.join(TARGET_LOCATION, filename), "wb")
        self.total_bytes_written = 0

    def __repr__(self) -> str:
        return f"<FileWriter {self.filename}: {human_bytes(self.total_bytes_written)}>"

    def write(self, data: bytes) -> None:
        self.fi.write(data)
        self.total_bytes_written += len(data)
        if len(data) > 2:
            print(f"Wrote {self.filename} - {self.total_bytes_written}")

    def close(self) -> None:
        print(f"Closing {self.filename} - {self.total_bytes_written}")
        self.fi.close()


START_STREAM_MATCH = b"\x00\x00\x00\x08wide***\xf8mdat"

_start = time.time()


def log(msg: str, nl=True) -> None:
    prefix = f"[{human_time(time.time() - _start)}] "
    postfix = "         ----"
    if nl:
        print(prefix + msg + postfix)
    else:
        print(prefix + msg + postfix, end="\r")


_position_filename = "position.json"


def record_position(disk_reader: DiskReader, file_num: int) -> None:
    with open(_position_filename, "w") as fi:
        fi.write(
            json.dumps(
                {"bytes_read": disk_reader.total_bytes_read, "file_num": file_num}
            )
        )


def read_position() -> dict:
    if not os.path.exists(_position_filename):
        return {"bytes_read": 0, "file_num": 0}
    with open(_position_filename, "r") as fi:
        return json.loads(fi.read())


def run():
    last6 = b""
    found_file_num = 0
    current_file = None
    current_match_pos = 0
    match_buffer = b""
    with open(SOURCE_DISK, "rb") as disk:
        fi_reader = DiskReader(disk)

        if RESTART_FROM_POSITION:
            pos = read_position()
            found_file_num = pos["file_num"]
            fi_reader.seek(pos["bytes_read"])

        # for byte in fi_reader:
        for byte in fi_reader.read_all():
            if fi_reader.total_bytes_read % 1000000 == 0:
                log(f"Reading {fi_reader}, Recovering: {current_file}", nl=False)
            if current_file is None and fi_reader.total_bytes_read % 10000000 == 0:
                record_position(fi_reader, found_file_num)

            if current_file is not None:
                last6 = last6[-5:] + byte
                if last6[0:1] == b"\x00" and last6.endswith(b"data"):
                    datum_size = last6[1]
                    datum = fi_reader.read(datum_size + 3)
                    log(f"Found datum: {datum_size} {len(datum)} {[v for v in datum]}")
                    current_file.write(byte + datum)
                    if not fi_reader.peek(5).endswith(b"data"):
                        log(f"End of file found: {current_file}")
                        current_file.close()
                        current_file = None
                else:
                    current_file.write(byte)
            elif (
                byte == START_STREAM_MATCH[current_match_pos : current_match_pos + 1]
                or START_STREAM_MATCH[current_match_pos : current_match_pos + 1] == b"*"
            ):
                current_match_pos += 1
                match_buffer += byte
                if current_match_pos == len(START_STREAM_MATCH):
                    found_file_num += 1
                    current_match_pos = 0
                    filename = f"found-file-{found_file_num}.braw"
                    current_file = FileWriter(filename)
                    current_file.write(match_buffer)
                    log(f"Found file! starting write out: {current_file}")
                    match_buffer = b""
            else:
                current_match_pos = 0
                match_buffer = b""

    print("Done!")


if __name__ == "__main__":
    run()
