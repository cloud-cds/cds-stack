import hashlib


key = 'shhh-this is a secret!'

hash_object = hashlib.sha1(key.encode('utf-8'))
dig = bytearray(hash_object.digest())
hex_dig = hash_object.hexdigest()
print(hex_dig)
for d in dig:
    print(d)


key_size = 128
hashed_key_1 = bytearray('\x00' * 64)
hashed_key_2 = bytearray('\x00' * 64)
print("len:", len(hashed_key_1))
for i in range(64):
    if i < len(dig):
        hashed_key_1[i] = dig[i] ^ 0x36
    else:
        hashed_key_1[i] = 0x36

for i in range(64):
    if i < len(dig):
        hashed_key_2[i] = dig[i] ^ 0x5c
    else:
        hashed_key_2[i] = 0x5c

hash_object = hashlib.sha1(hashed_key_1)
hashed_key_1 = bytearray(hash_object.digest())

hash_object = hashlib.sha1(hashed_key_2)
hashed_key_2 = bytearray(hash_object.digest())


hashed_key = hashed_key_1 + hashed_key_2
print("len:", len(hashed_key))
print("hashed_key")
for k in hashed_key[:16]:
    print(k)
import binascii

print(binascii.hexlify(hashed_key[:16]))



