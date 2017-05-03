import hashlib
import binascii
import StringIO

querystring_key = os.environ['querystring_key'] if 'querystring_key' in os.environ else None

def hash_password(key, key_size):
  hash_object = hashlib.sha1(key.encode('utf-8'))
  dig = bytearray(hash_object.digest())
  hex_dig = hash_object.hexdigest()
  hashed_key_1 = bytearray(b'\x36' * 64)
  hashed_key_2 = bytearray(b'\x5c' * 64)

  for i in range(len(dig)):
    hashed_key_1[i] = dig[i] ^ 0x36
    hashed_key_2[i] = dig[i] ^ 0x5c

  hash_object = hashlib.sha1(hashed_key_1)
  hashed_key_1 = bytearray(hash_object.digest())

  hash_object = hashlib.sha1(hashed_key_2)
  hashed_key_2 = bytearray(hash_object.digest())

  hashed_key = hashed_key_1 + hashed_key_2
  return binascii.hexlify(hashed_key[:(key_size / 8)])

aes = AES.new(hash_password(querystring_key), AES.MODE_CBC, b'\x00' * 16) if querystring_key else None
pkcs7_encoder = PKCS7Encoder()

def decrypt(encrypted_text):
  if aes:
    encrypted_text = urllib.unquote(encrypted_text)
    decodetext =  base64.b64decode(encrypted_text)
    cipher = aes.decrypt(decodetext)
    pad_text = pkcs7_encoder.decode(cipher)
    return pad_text

  return None

class PKCS7Encoder(object):
  def __init__(self, k=16):
    self.k = k

  ## @param text The padded text for which the padding is to be removed.
  # @exception ValueError Raised when the input padding is missing or corrupt.
  def decode(self, text):
    '''
    Remove the PKCS#7 padding from a text string
    '''
    nl = len(text)
    val = int(binascii.hexlify(text[-1]), 16)
    if val > self.k:
        raise ValueError('Input is not padded or padding is corrupt')

    l = nl - val
    return text[:l]

  ## @param text The text to encode.
  def encode(self, text):
    '''
    Pad an input string according to PKCS#7
    '''
    l = len(text)
    output = StringIO.StringIO()
    val = self.k - (l % self.k)
    for _ in xrange(val):
        output.write('%02x' % val)
    return text + binascii.unhexlify(output.getvalue())
