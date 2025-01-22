from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex
import sys


class Prpcrypt:
    def __init__(self, key):
        if len(key) not in [16, 24, 32]:
            raise ValueError("Key Length must be 16, 24,  or 32 bytes.")
        self.key = key.encode("utf-8")
        self.mode = AES.MODE_CBC

    def encrypt(self, text):
        cryptor = AES.new(self.key, self.mode, b"0000000000000000")
        length = 16
        count = len(text)
        add = length - (count % length)
        text = text + ("\0" * add)
        text = text.encode("utf-8")
        self.ciphertext = cryptor.encrypt(text)
        bin_text = b2a_hex(self.ciphertext)
        return bin_text.decode("utf-8")

    def decrypt(self, text):
        text = text.encode("utf-8")
        cryptor = AES.new(self.key, self.mode, b"0000000000000000")
        plain_text = cryptor.decrypt(a2b_hex(text))
        return plain_text.rstrip(b"\0").decode("utf-8")


if __name__ == "__main__":
    pass
