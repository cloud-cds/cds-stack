
from Crypto.Cipher import AES
from pkcs7 import PKCS7Encoder
import base64
key = 'qwerty12345'




hashed_key = '\xC8\xED\x91\x1A\x89\x07\xEF\xE4\xC1\xDE\x24\xCA\x67\xDF\x5F\xA2'

secret_text = 'user=andong'
mode = AES.MODE_CBC
IV = '\x00' * 16

encoder = PKCS7Encoder()
padded_text = encoder.encode(secret_text)
print padded_text
e = AES.new(hashed_key, mode, IV)

cipher_text = e.encrypt(padded_text)

encrypted_text = base64.b64encode(cipher_text)

#encrypted_text = 'FarfiPlFL4zdTLjDu6sgOw=='
print encrypted_text



decodetext =  base64.b64decode(encrypted_text)
aes = AES.new(hashed_key, mode, IV)
cipher = aes.decrypt(decodetext)
pad_text = encoder.decode(cipher)
print pad_text
# clear_text = PKCS7Encoder().decode(e.decrypt(base64.b64decode(cipher_text)))

# print clear_text
encrypted_text = "xmV4yrj3MYe6A0SbNuo5%2F%2BGbNfKXRBBD545kEXXwiwspE9lMAVDs9uitKDi2n5hK"

hashed_key = '\xe7\xcd\xe8\x12\x26\xf1\xd5\xe0\x3c\x26\x81\x03\x56\x92\x96\x4d'

import urllib
decodetext =  base64.b64decode(urllib.unquote(encrypted_text))
aes = AES.new(hashed_key, mode, IV)
cipher = aes.decrypt(decodetext)
pad_text = encoder.decode(cipher)
print pad_text
