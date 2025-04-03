from resources.dev import config  # Importing configuration values (key, iv, salt) from the config module.
# from logging_config import logger  # This line is commented out. If logging is needed, it can be used for error handling.


# AES (Advanced Encryption Standard) encryption using AES-CBC (Cipher Block Chaining) mode.
# Below is a breakdown of how AES encryption is being applied in your code:

import os, sys
import base64
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2

try:
    # Fetching encryption parameters from the config module
    key = config.key  # Secret key used for encryption
    iv = config.iv  # Initialization vector for AES encryption
    salt = config.salt  # Salt value used in key derivation

    # Check if any of the required values are missing
    if not (key and iv and salt):
        raise Exception("Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occurred. Details: {e}")  # Print the error message
    # logger.error("Error occurred. Details: %s", e)  # Logging the error (commented out)
    sys.exit(0)  # Exit the program if key, iv, or salt is missing

# Block size for AES encryption
BS = 16

# Padding function to ensure data length is a multiple of BS
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')

# Unpadding function to remove added padding from decrypted text
unpad = lambda s: s[0:-ord(s[-1:])]

def get_private_key():
    """
    Derives a 32-byte private key using PBKDF2 (Password-Based Key Derivation Function 2).
    The derived key is used for AES encryption.
    """
    Salt = salt.encode('utf-8')  # Convert salt string to bytes
    kdf = PBKDF2(key, Salt, 64, 1000)  # Derive a 64-byte key using PBKDF2
    key32 = kdf[:32]  # Use the first 32 bytes for AES encryption
    return key32

def encrypt(raw):
    """
    Encrypts the given plaintext string using AES encryption in CBC mode.
    The input text is padded to match the AES block size.
    """
    raw = pad(raw)  # Pad the input text
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))  # Create AES cipher object
    return base64.b64encode(cipher.encrypt(raw))  # Encrypt and return base64-encoded ciphertext

def decrypt(enc):
    """
    Decrypts the given encrypted text using AES encryption in CBC mode.
    The input is base64-decoded before decryption.
    """
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))  # Create AES cipher object
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')  # Decrypt and remove padding





