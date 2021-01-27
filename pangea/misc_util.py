#-*- coding: utf-8 -*-
"""  Общие утилитки
"""
# $Id: misc_util.py 9167 2009-06-04 14:29:06Z efremov $

__version__ = '$Revision: 9167 $'[11:-2]

DEF_ZACCUR = 1.0e-5  # default accuracy when searching table with bisect

import traceback
from . import koi2volapyuk
import string
from bisect import bisect

sql_safe_special_char=' _-+=()[]$#@!|/?.,<>:'
sql_normal_symbols = string.ascii_letters + string.digits + sql_safe_special_char + koi2volapyuk.letters
file_name_normal_symbols = string.ascii_letters + string.digits + '_-+=.@:'

MAXFLOAT = 3.40282347e+38 ## stands for undefined values of parameters

def fileline():
    """
    Возвращает текст 'DEBUG название файла, номер строки' с названием файла и номером 
    строки, откуда ее вызвали
    """
    stack = traceback.extract_stack()
    stack.pop()
    info = stack.pop()
    return "DEBUG " + str(info[0]) + "," + str(info[1])

def normalize_name(name):
    """Normalize name so it is usable for file names
    """
    s = koi2volapyuk.rus2volapyuk(name)
    lst = list(map(correct_letter, s))
    s1 = ''.join(lst)
    return s1

def normalize_4_sql(s):
    """Normalize name so it is usable in sql operators
    """
    s1 = s
    lst = list(map(sql_correct_letter, s1))
    s1 = ''.join(lst)
    return s1

def correct_letter(e):
    if e in file_name_normal_symbols:
        return e
    else:
        return '_'
    
def sql_correct_letter(e):
    "Return its argument if character is safe to use in SQL strings, return _ otherwise. Cyrillic letters are considered normal."
    if e in sql_normal_symbols:
        return e
    else:
        return '_'

def stringIsCorrect(s):
    """Check if all chars in string are correct for file names.
    Returns:
      1 if the condition is met, 0 otherwise.
    """
    for c in s:
        if not (c in file_name_normal_symbols):
            return 0
    return 1

def stringIsSQLCorrect(s):
    """Check if all chars in string are correct for SQL strings.
    Returns:
      1 if the condition is met, 0 otherwise.
    """
    for c in s:
        if not (c.isalnum() or  (c in sql_safe_special_char) ) :
            return 0
    return 1


def makeUniqueNameNew(name, nm_list, separator, max_try = 100):
    """Makes new name not in the list nm_list.
    Unique name if made by adding integer index prefixed with separator
    to the end of the name. In the case the name is already composite
    name with separator and index at the end, index is incremented. E.g.
    f0_1 may result in f0_2 (provided separator is _)
    """
    nm_list_upper = [n.upper() for n in nm_list]
    if not (name.upper() in nm_list_upper):  
        return name
    # try to split name
    ndx = name.rfind(separator)
    if ndx == -1:
        start_ndx = 1
    else:
        try_start_ndx_s = name[ndx+1:]
        try_start_ndx = None
        try:
            try_start_ndx = int(try_start_ndx_s)
        except ValueError:
            start_ndx = 1
        else:
            name = name[:ndx]
            start_ndx = try_start_ndx + 1
        
    for i in range(start_ndx, max_try):
        try_nm = name + ("%s%i" % ( separator, i))
        if not (try_nm.upper() in nm_list_upper):
            return try_nm
    raise RuntimeError('Maximum number of tries %d reached' % max_try)

def makeUniqueName(name, nm_list, separator, max_try = 100):
    """Makes new name not in the list nm_list.
    Unique name if made by adding integer index prefixed with separator
    to the end of the name. In the case the name is already composite
    name with separator and index at the end, index is incremented. E.g.
    f0_1 may result in f0_002 (provided separator is _)
    """
    if not (name in nm_list):  # !!! @TODO: should compare without upper/lower register!
        return name
    # try to split name
    ndx = name.rfind(separator)
    if ndx == -1:
        start_ndx = 1
    else:
        try_start_ndx_s = name[ndx+1:]
        try_start_ndx = None
        try:
            try_start_ndx = int(try_start_ndx_s)
        except ValueError:
            start_ndx = 1
        else:
            name = name[:ndx]
            start_ndx = try_start_ndx + 1
        
    for i in range(start_ndx, max_try):
        try_nm = name + ("%s%03i" % ( separator, i))
        if not (try_nm in nm_list):
            return try_nm
    raise RuntimeError('Maximum number of tries %d reached' % max_try)


def valueFromTableLinInter(table, z, ZACCUR = DEF_ZACCUR):
    """Find vaue from table. Table may contain more than 2 columns. 
    Using linear interpolation between 2 nearest values if there is no exact match. Return 
    MAXFLOAT or -MAXFLOAT.
    """
    val = MAXFLOAT
    ind = bisect(table, (z,))
    if ind >= len(table):
        if abs(z - table[-1][0]) < ZACCUR:
            return table[-1][1]
        return MAXFLOAT
    z0, v0 = table[ind][:2]
    if ind == 0:
        if abs(z - z0) < ZACCUR:
            return v0
        return -MAXFLOAT
    ind1 = ind - 1
    z1, v1 = table[ind1][:2]
    v = (v1 * (z0 - z) + v0 * (z - z1)) / (z0 - z1)
    return v


##########################
if __name__ == "__main__":
    # tesing:
    print("fileline = ",fileline())
    #
    # ***************** makeUniqueName ******************
    nm = 'f0'
    l = ['f1', 'f2', 'f3']
    sep = '_'
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))

    nm = 'f1'
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))
    l += ['f0_1']
    nm = 'f0_1'
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))
    l += ['f0_002', 'f0_004', 'f_2', 'f_3']
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))

    nm = 'F1'
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))
    l += ['f0_1']
    nm = 'F0_1'
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))
    l += ['f0_002', 'f0_004', 'f_2', 'f_3']
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))
    print(makeUniqueNameNew(nm, l, sep))
    print("makeUniqueName in", nm, l, sep)

    nm = 'f0_1'
    ntries = 1
    print("makeUniqueName in", nm, l, sep, ntries)
    try:
        print(makeUniqueName(nm, l, sep, ntries))
    except RuntimeError:
        print("Passed")
    else:
        raise RuntimeError("Should be exception here!")
    sep = ' '
    print("makeUniqueName in", nm, l, sep)
    print(makeUniqueName(nm, l, sep))

    # testing normalization functions:
    tstr1 = "abcdabcd!@#$%^&*()_,.<>/?;:'\"\\|-+=abcd "
    print("normalize_name", normalize_name(tstr1))
    assert (normalize_name(tstr1) ==  'abcdabcd_@__________.__________-+=abcd_')
    print("normalize_4_sql", normalize_4_sql(tstr1))
    assert (normalize_4_sql(tstr1) == 'abcdabcd!@#$____()_,.<>/?_____|-+=abcd ')
    tstr2 = tstr1 + koi2volapyuk.ruslett
    print("normalize_name", normalize_name(tstr2))
    assert (normalize_name(tstr2) == 'abcdabcd_@__________.__________-+=abcd_abwgdeyozhziklmnoprstufhcchshschyeyuyaj')
    print("normalize_4_sql", normalize_4_sql(tstr2))
    assert (normalize_4_sql(tstr2) == 'abcdabcd!@#$____()_,.<>/?_____|-+=abcd ' + koi2volapyuk.ruslett)
    
    assert (stringIsSQLCorrect(normalize_4_sql(tstr2)))

    # finding values from tables
    table = [(3100.0, -2647.0), (3100.1999999999998, -2647.0999999999999), (3100.4000000000001, -2647.21), (3100.5999999999999, -2647.3099999999999), (3100.8000000000002, -2647.4200000000001), (3101.0, -2647.5300000000002), (3101.1999999999998, -2647.6300000000001), (3101.4000000000001, -2647.73), (3101.5999999999999, -2647.8299999999999), (3101.8000000000002, -2647.9299999999998), (3448.5, -2834.4200000000001), (3448.5999999999999, -2834.4899999999998), (3448.6999999999998, -2834.5500000000002), (3448.8000000000002, -2834.6199999999999), (3448.9000000000001, -2834.6900000000001), (3449.0, -2834.75), (3449.0999999999999, -2834.8200000000002), (3449.1999999999998, -2834.8899999999999), (3449.3000000000002, -2834.9499999999998), (3449.4000000000001, -2835.02)]
    assert (valueFromTableLinInter(table, 3100.) == -2647.0)
    assert (valueFromTableLinInter(table, 3000.) == -MAXFLOAT)
    assert (valueFromTableLinInter(table, 3500.) == MAXFLOAT)
    assert (abs(valueFromTableLinInter(table, 3200.) + 2700.7517998269395) < 1.0e-8)
