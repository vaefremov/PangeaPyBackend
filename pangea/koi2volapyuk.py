#!/opt/PANGmisc/bin/python
#  $Id: koi2volapyuk.py 7547 2008-10-23 07:23:05Z efremov $
#
"""
Package to translate russian KOI string to its Volyapyuk
equivalent.
"""
__version__ = '$Revision: 7547 $'[11:-2]

import sys
import string
import codecs
import types

caps = b'\xe1\xe2\xf7\xe7\xe4\xe5\xb3\xf6\xfa\xe9\xea\xeb\xec\xed\xee\xef\xf0\xf2\xf3\xf4\xf5\xe6\xe8\xe3\xfe\xfb\xfd\xf8\xff\xf9\xfc\xe0\xf1'
lett = b'\xc1\xc2\xd7\xc7\xc4\xc5\xa3\xd6\xda\xc9\xca\xcb\xcc\xcd\xce\xcf\xd0\xd2\xd3\xd4\xd5\xc6\xc8\xc3\xde\xdb\xdd\xd8\xdf\xd9\xdc\xc0\xd1'

ruscaps = str(caps, 'koi8-r')
ruslett = str(lett, 'koi8-r')
letters = ruscaps + ruslett

tr_table = {
b'\xb3'[0]: 'Yo',
b'\xa3'[0]: 'yo',
b'\xf6'[0]: 'Zh',
b'\xd6'[0]: 'zh',
b'\xfe'[0]: 'Ch',
b'\xde'[0]: 'ch',
b'\xfb'[0]: 'Sh',
b'\xdb'[0]: 'sh',
b'\xfd'[0]: 'Sch',
b'\xdd'[0]: 'sch',
b'\xf8'[0]: '',
b'\xd8'[0]: '',
b'\xff'[0]: '',
b'\xdf'[0]: '',
b'\xfc'[0]: 'E',
b'\xdc'[0]: 'e',
b'\xe0'[0]: 'Yu',
b'\xc0'[0]: 'yu',
b'\xf1'[0]: 'Ya',
b'\xd1'[0]: 'ya',
b'\xea'[0]: 'J',
b'\xca'[0]: 'j',
}

def koi2volapyuk(in_str):
    """Translation procedure.
    Input: string in KOI
    Output: translated string"""
    assert (type(in_str) == bytes), "ERROR: koi2volapyuk does not support unicode input"
    global tr_table
    l = []
    for c in in_str:
        if c > 128:
            try:
                rc = tr_table[c]
            except KeyError:
                # default translation
                rc = chr(c & 0x7f).swapcase()
        else:
            rc = chr(c)
        l.append(rc)
    return ''.join(l)

def rus2volapyuk(in_str):
    # here only russian chars are supported
    # (UnicodeEncodeError exception will be thrown otherwise)
    tmp_str = codecs.encode(in_str, 'koi8-r')
    return koi2volapyuk(tmp_str)

if __name__ == '__main__':
    caps_should_be = 'ABWGDEYoZhZIJKLMNOPRSTUFHCChShSchYEYuYa'
    res = koi2volapyuk(caps)
    print(res)
    if res != caps_should_be:
        print('ERROR: translation failed\n', caps_should_be, '\n', res)
        sys.exit(1)
    print("koi2volapyuk(caps) passed")

    small_should_be = 'abwgdeyozhzijklmnoprstufhcchshschyeyuya'
    res = koi2volapyuk(lett)
    print(res)
    if res != small_should_be:
        print('ERROR: translation failed')
        sys.exit(1)
    print("koi2volapyuk(lett) passed")

    latin_should_be = 'ABCDabcd'
    res = koi2volapyuk(b'ABCDabcd')
    print(res)
    if res != latin_should_be:
        print('ERROR: translation failed')
        sys.exit(1)
    print("koi2volapyuk(latin) passed")
    try:
        koi2volapyuk(ruslett)
    except AssertionError as ex:
        print("Unicode test passed:", ex)
    else:
        raise AssertionError("Exception should be here")

    # Testing utf2volapyuk:
    res = rus2volapyuk(ruslett)
    print(res)
    if res != small_should_be:
        print('ERROR: translation failed')
        sys.exit(1)
    print("koi2volapyuk(unicode) passed")
