import os
import io
import hashlib
import binascii
import base64
import logging
import urllib.parse
from Crypto.Cipher import AES

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
  return hashed_key[:16]

class PKCS7Encoder(object):
  """
  Python implementation of PKCS #7 padding.
  RFC 2315: PKCS#7 page 21
  Some content-encryption algorithms assume the
  input length is a multiple of k octets, where k > 1, and
  let the application define a method for handling inputs
  whose lengths are not a multiple of k octets. For such
  algorithms, the method shall be to pad the input at the
  trailing end with k - (l mod k) octets all having value k -
  (l mod k), where l is the length of the input. In other
  words, the input is padded at the trailing end with one of
  the following strings:
           01 -- if l mod k = k-1
          02 02 -- if l mod k = k-2
                      .
                      .
                      .
        k k ... k k -- if l mod k = 0
  The padding can be removed unambiguously since all input is
  padded and no padding string is a suffix of another. This
  padding method is well-defined if and only if k < 256;
  methods for larger k are an open issue for further study.
  """

  # Original Source: http://japrogbits.blogspot.com/2011/02/using-encrypted-data-between-python-and.html

  # Updated to Python 3.
  #   Despite some complaints of Python 3, using it correctly simplified the
  #   original greatly.

  def __init__(self, k=16):
    self.k = k


  ## @param bytestring    The padded bytestring for which the padding is to be removed.
  ## @param k             The padding block size.
  # @exception ValueError Raised when the input padding is missing or corrupt.
  # @return bytestring    Original unpadded bytestring.
  def decode(self, bytestring):
    """
    Remove the PKCS#7 padding from a text bytestring.
    """

    val = bytestring[-1]
    if val > self.k:
        raise ValueError('Input is not padded or padding is corrupt')
    l = len(bytestring) - val
    return bytestring[:l]


  ## @param bytestring    The text to encode.
  ## @param k             The padding block size.
  # @return bytestring    The padded bytestring.
  def encode(self, bytestring):
    """
    Pad an input bytestring according to PKCS#7
    """
    l = len(bytestring)
    val = self.k - (l % self.k)
    return bytestring + bytearray([val] * val)


pkcs7_encoder = PKCS7Encoder()
encrypted_query = False
hash_key = None

if querystring_key:
  encrypted_query = True
  hash_key = bytes(hash_password(querystring_key, 128))

def encrypt(text):
  if hash_key:
    aes = AES.new(hash_key, AES.MODE_CBC, IV=('\x00' * 16)) if querystring_key else None
    padded = pkcs7_encoder.encode(text.encode('utf-8'))
    encrypted = aes.encrypt(padded)
    encoded = base64.b64encode(encrypted)
    return encoded

  return None

def decrypt(encrypted):
  if hash_key:
    aes = AES.new(hash_key, AES.MODE_CBC, IV=('\x00' * 16)) if querystring_key else None
    decoded = base64.b64decode(encrypted)
    logging.info('Decoded: %s' % str(decoded))
    decrypted = aes.decrypt(decoded)
    logging.info('Decrypted: %s' % str(decrypted))
    unpadded = pkcs7_encoder.decode(decrypted)
    logging.info('Unpadded: %s' % str(unpadded))
    return unpadded
