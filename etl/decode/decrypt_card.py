"""
Credits:
akintos: https://gist.github.com/akintos/04e2494c62184d2d4384078b0511673b
timelic: https://github.com/timelic/master-duel-chinese-translation-switch
"""

from typing import List
import json
import os
import sys
import zlib

# 0. Definitions


def FileCheck(filename):
    try:
        open(filename, "r")
        return 1
    except IOError:
        print(f"Error: File {filename} does not appear to exist.")
        return 0


def Decrypt(data: bytes, m_iCryptoKey):
    data = bytearray(data)
    try:
        for i in range(len(data)):
            v = i + m_iCryptoKey + 0x23D
            v *= m_iCryptoKey
            v ^= i % 7
            data[i] ^= v & 0xFF
        return zlib.decompress(data)
    except zlib.error:
        # print('zlib.error because of wrong crypo key:' + hex(m_iCryptoKey))
        return bytearray()
    # except Exception:
    # else:


def ReadByteData(filename):
    with open(f"{filename}", "rb") as f:
        data = f.read()
    f.close()
    return data


def WriteDecByteData(filename, data):
    with open(f"{filename}" + ".dec", "wb") as f:
        f.write(data)
    f.close()


def CheckCryptoKey(filename, m_iCryptoKey):
    data = ReadByteData(filename)
    if Decrypt(data, m_iCryptoKey) == bytearray():
        return 0
    else:
        return 1


def FindCryptoKey(filename):
    print("No correct crypto key found. Searching for crypto key...")
    m_iCryptoKey = -0x1
    data = ReadByteData(filename)
    dec_data = bytearray()
    while dec_data == bytearray():
        m_iCryptoKey = m_iCryptoKey + 1
        dec_data = Decrypt(data, m_iCryptoKey)
        # if os.stat('CARD_Indx.dec').st_size > 0:
    with open("!CryptoKey.txt", "w") as f_CryptoKey:
        f_CryptoKey.write(hex(m_iCryptoKey))
    f_CryptoKey.close()
    print(
        'Found correct crypto key "'
        + hex(m_iCryptoKey)
        + '" and wrote it to file "!CryptoKey.txt".'
    )
    return m_iCryptoKey


def GetCryptoKey(filename):
    if FileCheck("!CryptoKey.txt") == 1:
        print("Trying to read crypto key from file...")
        with open("!CryptoKey.txt", "rt") as f_CryptoKey:
            m_iCryptoKey = int(f_CryptoKey.read(), 16)
        f_CryptoKey.close()
        print(
            'Reading crypto key "'
            + hex(m_iCryptoKey)
            + '" from file, checking if it is correct...'
        )
    else:
        m_iCryptoKey = 0x0

    if CheckCryptoKey(filename, m_iCryptoKey) == 1:
        print('The crypto key "' + hex(m_iCryptoKey) + '" is correct.')
    else:
        m_iCryptoKey = FindCryptoKey(filename)
    return m_iCryptoKey


def Check_files(Filename_list):
    Checked_filename_list = []
    File_found_list = []

    for i in range(len(Filename_list)):
        Filename = Filename_list[i]

        if FileCheck(Filename) == 1:
            Checked_filename_list.append(Filename)
        elif FileCheck(Filename + ".bytes") == 1:
            Checked_filename_list.append(Filename + ".bytes")
        elif FileCheck(Filename + ".txt") == 1:
            Checked_filename_list.append(Filename + ".txt")

    return Checked_filename_list


def WriteJSON(l: list, json_file_path: str):
    with open(json_file_path, "w", encoding="utf8") as f:
        json.dump(l, f, ensure_ascii=False, indent=4)


# The start of Name and Desc is 0 and 4 respectively
def ProgressiveProcessing(CARD_Indx_filename, filename, start):

    # Read binary index
    with open(CARD_Indx_filename + ".dec", "rb") as f:
        hex_str_list = (
            "{:02X}".format(int(c)) for c in f.read()
        )  # Define variables to accept file contents
    dec_list = [int(s, 16) for s in hex_str_list]  # Convert hexadecimal to decimal

    # Get the index of Desc
    indx = []
    for i in range(start, len(dec_list), 8):
        tmp = []
        for j in range(4):
            tmp.append(dec_list[i + j])
        indx.append(tmp)

    def FourToOne(x: List[int]) -> int:
        res = 0
        for i in range(3, -1, -1):
            res *= 16 * 16
            res += x[i]
        return res

    indx = [FourToOne(i) for i in indx]
    indx = indx[1:]

    # Convert Decrypted CARD files to JSON files
    def Solve(data: bytes, desc_indx: List[int]):
        res = []
        for i in range(len(desc_indx) - 1):
            s = data[desc_indx[i] : desc_indx[i + 1]]
            s = s.decode("UTF-8", "ignore")
            while len(s) > 0 and s[-1] == "\u0000":
                s = s[:-1]
            res.append(s)
        return res

    # Read Desc file
    with open(f"{filename}" + ".dec", "rb") as f:
        data = f.read()

    desc = Solve(data, indx)

    WriteJSON(desc, f"{filename}" + ".dec.json")


def decrypt_desc_indx_name():

    # 1. Check if CARD_* files exist:

    CARD_filename_list = Check_files(
        [
            "./etl/services/temp/card_indx.bytes",
            "./etl/services/temp/card_name.bytes",
            "./etl/services/temp/card_desc.bytes",
        ]
    )

    print(CARD_filename_list)
    CARD_Indx_filename = CARD_filename_list[0]
    CARD_Name_filename = CARD_filename_list[1]
    CARD_Desc_filename = CARD_filename_list[2]

    # 2. Get crypto key

    m_iCryptoKey = GetCryptoKey(CARD_Indx_filename)

    # 3. Decrypt card files from section 1

    print("Decrypting files...")

    for filename in CARD_filename_list:
        data = ReadByteData(filename)
        data = Decrypt(data, m_iCryptoKey)
        WriteDecByteData(filename, data)
        print('Decrypted file "' + filename + '".')

    # 4. Split CARD_Name + CARD_Desc

    print("Splitting files...")

    ProgressiveProcessing(CARD_Indx_filename, CARD_Name_filename, 0)
    ProgressiveProcessing(CARD_Indx_filename, CARD_Desc_filename, 4)

    print("Finished splitting files.")
