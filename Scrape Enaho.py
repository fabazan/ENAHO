###########################################################################################################
# Peruvian Households Dataset
# Author: Sebastian Sardon
# Last updated: June 16, 2019
# Retrieves raw ENAHO data from INEI's official website
# Reference period: 1997-2018 (these are all the years for which complete surveys are available)
###########################################################################################################


import numpy as np
import pandas as pd
import os
import shutil
import glob
import time
import zipfile
from urllib.request import urlretrieve

from simpledbf import Dbf5
os.system("cls")

# Codes for surveys of the class "ENAHO Metodología ACTUALIZADA"
# These are rather unstructured, so codes are obtained manually from INEI's webpage
survey_codes = {"enaho_1997":  "04", "enaho_1998":  "08", "enaho_1999":  "13", "enaho_2000":  "30",
                "enaho_2001":  "52", "enaho_2002":  "91", "enaho_2003":  "31", "enaho_2004": "280", "enaho_2005": "281",
                "enaho_2006": "282", "enaho_2007": "283", "enaho_2008": "284", "enaho_2009": "285", "enaho_2010": "279",
                "enaho_2011": "291", "enaho_2012": "324", "enaho_2013": "404", "enaho_2014": "440", "enaho_2015": "498",
                "enaho_2016": "546", "enaho_2017": "603", "enaho_2018": "634", "enaho_2019": "687", "enaho_2020": "737",
                "enaho_2021": "759"}

survey_codes_list = list(survey_codes); survey_codes_list.sort()
starting_year, ending_year = int(survey_codes_list[0][-4:]), int(survey_codes_list[-1][-4:])

# Module codes
#  01: Características de la Vivienda y del Hogar
#  02: Características de los Miembros del Hogar
#  03: Educación
#  05: Empleo e Ingresos
#  34: Sumarias
#  85: Gobernabilidad, Democracia y Transparencia
mod_codes = ["01", "02", "03", "05", "34", "85"]

# zip file sizes for each module - year to compare if the zip files are corrupted where they exist
zip_files_sizes = {"module 01 1997.zip": 998986, "module 01 1998.zip": 2156374, "module 01 1999.zip": 2412496, "module 01 2000.zip": 1411073, "module 01 2001.zip": 2307071, "module 01 2002.zip": 3131795, "module 01 2003.zip": 2314943, "module 01 2004.zip": 5845230, "module 01 2005.zip": 6217110,"module 01 2006.zip": 6074823, "module 01 2007.zip": 2731404, "module 01 2008.zip": 5621781, "module 01 2009.zip": 5886584, "module 01 2010.zip": 6251896, "module 01 2011.zip": 5887755, "module 01 2012.zip": 4609628, "module 01 2013.zip": 6691088, "module 01 2014.zip": 6006287, "module 01 2015.zip": 6548376, "module 01 2016.zip": 10286386, "module 01 2017.zip": 11483823,  "module 01 2018.zip": 11330023, "module 01 2019.zip": 9796304, "module 01 2020.zip": 12427173, "module 01 2021.zip": 18811541,
                   "module 02 1997.zip": 710829, "module 02 1998.zip": 519484, "module 02 1999.zip": 1216668,"module 02 2000.zip": 1210526, "module 02 2001.zip": 2679855, "module 02 2002.zip": 2767846, "module 02 2003.zip": 2050978, "module 02 2004.zip": 5425067, "module 02 2005.zip": 5554344, "module 02 2006.zip": 5599278,"module 02 2007.zip": 1662487, "module 02 2008.zip": 4844115, "module 02 2009.zip": 5206315, "module 02 2010.zip": 5333001, "module 02 2011.zip": 2078531, "module 02 2012.zip": 5282669, "module 02 2013.zip": 4269721, "module 02 2014.zip": 4311965, "module 02 2015.zip": 5208221, "module 02 2016.zip": 8578312, "module 02 2017.zip": 9592141, "module 02 2018.zip": 9452896, "module 02 2019.zip": 7692256, "module 02 2020.zip": 9668085, "module 02 2021.zip": 16456442,
                   "module 03 1997.zip": 1091808, "module 03 1998.zip": 1035155, "module 03 1999.zip": 1522417, "module 03 2000.zip": 1722916, "module 03 2001.zip": 2983579, "module 03 2002.zip": 4823710, "module 03 2003.zip": 3376517, "module 03 2004.zip": 7690317, "module 03 2005.zip": 7871238, "module 03 2006.zip": 7813575, "module 03 2007.zip": 4555546, "module 03 2008.zip": 7686366, "module 03 2009.zip": 7736099, "module 03 2010.zip": 7761152, "module 03 2011.zip": 4785663, "module 03 2012.zip": 8151963, "module 03 2013.zip": 7064703, "module 03 2014.zip": 8175562, "module 03 2015.zip": 9278694, "module 03 2016.zip": 14349253, "module 03 2017.zip": 15343109, "module 03 2018.zip": 15579223, "module 03 2019.zip": 14361996, "module 03 2020.zip": 15221133, "module 03 2021.zip": 22276509,
                   "module 05 1997.zip": 1875548, "module 05 1998.zip": 959554, "module 05 1999.zip": 1459207, "module 05 2000.zip": 1512632, "module 05 2001.zip": 5386479, "module 05 2002.zip": 6904329, "module 05 2003.zip": 5470768, "module 05 2004.zip": 10374012, "module 05 2005.zip": 10238373, "module 05 2006.zip": 10656179, "module 05 2007.zip": 7675813, "module 05 2008.zip": 10110921, "module 05 2009.zip": 10563481, "module 05 2010.zip": 10760082, "module 05 2011.zip": 8315358, "module 05 2012.zip": 10626563, "module 05 2013.zip": 12580247, "module 05 2014.zip": 12669688, "module 05 2015.zip": 14356675, "module 05 2016.zip": 18734671, "module 05 2017.zip": 19334755, "module 05 2018.zip": 21603647, "module 05 2019.zip": 19641334, "module 05 2020.zip": 20070313, "module 05 2021.zip": 28400153,
                   "module 34 1997.zip": 1051008, "module 34 1998.zip": 905359, "module 34 1999.zip": 1391185, "module 34 2000.zip": 1423670, "module 34 2001.zip": 3900904, "module 34 2002.zip": 4870966, "module 34 2003.zip": 3195737, "module 34 2004.zip": 6751245, "module 34 2005.zip": 7015781, "module 34 2006.zip": 7009309, "module 34 2007.zip": 3902866, "module 34 2008.zip": 6482940, "module 34 2009.zip": 6751272, "module 34 2010.zip": 6775824, "module 34 2011.zip": 3737496, "module 34 2012.zip": 6697377, "module 34 2013.zip": 6226427, "module 34 2014.zip": 6052240, "module 34 2015.zip": 7136582, "module 34 2016.zip": 19145410, "module 34 2017.zip": 19161071, "module 34 2018.zip": 19960730, "module 34 2019.zip": 18257967, "module 34 2020.zip": 20963802, "module 34 2021.zip": 27996447,
                   "module 85 2002.zip": 4797877, "module 85 2003.zip": 3546688, "module 85 2004.zip": 7356682, "module 85 2005.zip": 7612428, "module 85 2006.zip": 7512342, "module 85 2007.zip": 1997373, "module 85 2008.zip": 5066268, "module 85 2009.zip": 5275541, "module 85 2010.zip": 5297053, "module 85 2011.zip": 2318528, "module 85 2012.zip": 5290146, "module 85 2013.zip": 4338045, "module 85 2014.zip": 3855346, "module 85 2015.zip": 4807052, "module 85 2016.zip": 8178562, "module 85 2017.zip": 8274573, "module 85 2018.zip": 9474252, "module 85 2019.zip": 7669606, "module 85 2020.zip": 9742989, "module 85 2021.zip": 17001958}


root = f"/Users/{os.getlogin()}/Desktop/workspace"
os.chdir(root)
print("Creating 'Trash' folder...")
try:
    os.mkdir("Trash")
except:
    print("   Folder already exists")


#################################
    # 1. Scrape zip files
#################################

def download_files():
    start_time = time.time()
    errors = []
    zip_files_found = [e[e.rfind("\\") + 1:] for e in glob.glob(root + "/Trash/*.zip")]
    print("Retrieving data...")
    for year in range(starting_year, ending_year + 1):
        for mod_code in mod_codes:
            zip_filename = f"module {mod_code} {year}.zip"
            print(f"   for year {year} - module {mod_code}...", end=" ")
            if (zip_filename in zip_files_found and
                    os.path.getsize(f"{root}/Trash/{zip_filename}") == zip_files_sizes[zip_filename]):
                print(f"file already exists")
            else:
                if zip_filename in zip_files_found:
                    os.remove(f"{root}/Trash/{zip_filename}")
                    print("replacing existing file...", end=" ")
                if year < 2004:
                    kind = "DBF"
                else:
                    kind = "STATA"
                url = f"http://iinei.inei.gob.pe/iinei/srienaho/descarga/{kind}/{survey_codes['enaho_'+str(year)]}-Modulo{mod_code}.zip"
                try:
                    urlretrieve(url, f"Trash/module {mod_code} {year}.zip")
                    print("done")
                except:
                    if year < 2003 and mod_code == "85":
                        print(f"not available")
                    else:
                        os.remove(f"{root}/Trash/{zip_filename}")
                        print(f"\n      ERROR: {zip_filename}")
                        print("      " + url)
                        errors.append(url)

    print(f"Scraping complete.")
    print(f"{len(errors)} errores: {errors}")
    elapsed = time.time() - start_time
    print(f"This took {round(elapsed)} s / {round(elapsed/60, 1)} min")
    input("Press enter to continue...")
    print()


##################################
    # 2. Extract zip files
###################################

def extract_files():
    start_time = time.time()
    errors = []
    try:
        shutil.rmtree(f"{root}/Enaho")
        os.mkdir(f"{root}/Enaho")
    except:
        os.mkdir(f"{root}/Enaho")
    os.mkdir(f"{root}/Enaho/in")
    os.mkdir(f"{root}/Enaho/in/Raw Data")
    for mod_code in mod_codes:
        new_dir = f"{root}/Enaho/in/Raw Data/module {mod_code}"
        try:
            shutil.rmtree(new_dir)
            os.mkdir(new_dir)
        except:
            os.mkdir(new_dir)
    print("Extracting data...")
    for year in range(starting_year, ending_year + 1):
        for mod_code in mod_codes:
            print(f"   for year {year} - module {mod_code}...", end=" ")
            if year < 2003 and mod_code == "85":
                print("\n      module 85 not available for this year")
                continue
            new_dir = f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}"
            try:
                shutil.rmtree(new_dir)
                os.mkdir(new_dir)
            except:
                os.mkdir(new_dir)
            zip_ref = zipfile.ZipFile(f"{root}/Trash/module {mod_code} {year}.zip")
            for file_name in zip_ref.namelist():
                try:
                    zip_ref.extract(file_name, f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}")
                except:
                    print(f"ERROR...", end=" ")
                    errors.append(f"module {mod_code} {year}.zip/{file_name}")
            zip_ref.close()
            print("done")
    print(f"Extraction complete.")
    print(f"{len(errors)} errores: {errors}")
    elapsed = time.time() - start_time
    print(f"This took {round(elapsed)} s / {round(elapsed/60, 1)} min")
    input("Press enter to continue...")
    print()


##################################################################################
    # 3. Remove redundant enclosing folder  (only a problem for some years)
##################################################################################

def remove_redundant():
    start_time = time.time()
    print("Removing redundant folders...")
    for year in range(starting_year, ending_year + 1):
        for mod_code in mod_codes:
            if year < 2003 and mod_code == "85":
                continue
            file_mod_year = f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}"
            file_tree = []
            for branch in os.walk(file_mod_year):
                file_tree.append(branch)
            if len(file_tree[0][1]) > 0:
                for file in file_tree[1][2]:
                    try:
                        shutil.move(f"{file_mod_year}/{file_tree[0][1][0]}/{file}", f"{file_mod_year}/{file}")
                    except:
                        print(f"   for year {year} - module {mod_code}...")
                        print(f"      could not move {file_mod_year}/{file_tree[0][1][0]}/{file}")

                shutil.rmtree(f"{file_mod_year}/{file_tree[0][1][0]}")
    print(f"Structuring complete.")
    elapsed = time.time() - start_time
    print(f"This took {round(elapsed)} s / {round(elapsed/60, 1)} min")
    input("Press enter to continue...")
    print()


#################################################################################
    # 4. Convert files for 1997-2003 ("ANTERIOR" class) from dbf to dta
#################################################################################

def convert_files():
    # Exception: module 05 files for years 2001-2003 are split into two dbf files (E1)
    def check_E1():
        return mod_code == "05" and (year == 2001 or year == 2002 or year == 2003)
    start_time = time.time()
    errors = []
    print("Converting dbf to dta...")
    for year in range(starting_year, ending_year + 1):
        for mod_code in mod_codes:
            if year < 2003 and mod_code == "85":
                continue
            os.chdir(f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}")
            # glob.glob() is case-insensitive on Windows
            dbf_list = list(set(glob.glob("*.dbf") + glob.glob("*.DBF")))
            if check_E1() is False:
                for file in dbf_list:
                    dbf_fn = file
                    try:
                        dta_fn = dbf_fn.split(".dbf")[0] + ".dta"
                        print(f"   working on {dbf_fn}...", end=" ")
                        df = Dbf5(dbf_fn).to_dataframe()
                        df.columns = [column.lower().replace("\x00", "").replace(" ", "")
                                      for column in df.columns]
                        df.to_stata(dta_fn)
                        print(f"done")
                        os.remove(dbf_fn)
                    except:
                        print(f"\n      ERROR in module {mod_code}/{year}/{dbf_fn}")
                        print(f"      {dbf_fn} bugged, must be converted manually")
                        errors.append(f"module {mod_code}/{year}/{dbf_fn}")
            else:
                dbf1_fn = dbf_list[0]
                dbf2_fn = dbf_list[1]
                try:
                    print(f"   working on {dbf1_fn} and {dbf2_fn}...", end=" ")
                    dta1_fn = f"{year}-1.dta"
                    dta2_fn = f"{year}-2.dta"
                    dta_fn = f"{year}.dta"
                    df1 = Dbf5(dbf1_fn).to_dataframe()
                    df2 = Dbf5(dbf2_fn).to_dataframe()
                    for current_df, current_dta in zip([df1, df2], [dta1_fn, dta2_fn]):
                        current_df.columns = [column.lower().replace("\x00", "").replace(" ", "")
                                              for column in current_df.columns]
                        current_df.to_stata(current_dta)
                    merge_vars = ["conglome", "vivienda", "hogar", "codperso"]
                    df = df1.merge(df2, on=merge_vars)
                    df.to_stata(dta_fn)
                    print(f"done")
                    os.remove(dbf1_fn); os.remove(dbf2_fn)
                except:
                    print(f"\n   ERROR: {dbf1_fn} and/or {dbf2_fn} bugged, must be converted manually")
                    print(f"      in: module {mod_code}/{year}/{dbf1_fn}")
                    print(f"      in: module {mod_code}/{year}/{dbf2_fn}")
                    errors.append(f"module {mod_code}/{year}/{dbf1_fn}")
    print(f"Data conversion complete.")
    print(f"{len(errors)} errores: {errors}")
    elapsed = time.time() - start_time
    print(f"This took {round(elapsed)} s / {round(elapsed/60, 1)} min")
    input("Press enter to continue...")
    print()


#####################################################
    # 5. Rename dta files for ease of looping
#####################################################

def rename_dta():
    start_time = time.time()
    print("Renaming dta files...")
    for year in range(starting_year, ending_year + 1):
        for mod_code in mod_codes:
            if mod_code == "85":  # we deal with files from this module in a special way: convert the "year-1" dataset
                if year < 2003:
                    continue
                else:
                    os.chdir(f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}")
                    dta_files = glob.glob(f"*{year}-1.dta")
                    os.rename(dta_files[0], f"{year}.dta")
                    print(f"   Renamed from {dta_files[0]} to {year}.dta")
            else:  # general criterion: rename (for use in Stata) the biggest file, ignore others
                os.chdir(f"{root}/Enaho/in/Raw Data/module {mod_code}/{year}")
                dta_files = list(set(glob.glob("*.dta") + glob.glob("*.DTA")))
                sizes = [os.path.getsize(file) for file in dta_files]
                index_max = np.argmax(sizes)
                os.rename(dta_files[index_max], f"{year}.dta")
                print(f"   Renamed from {dta_files[index_max]} to {year}.dta")
    print(f"Rename complete.")
    elapsed = time.time() - start_time
    print(f"This took {round(elapsed)} s / {round(elapsed/60, 1)} min")
    input("Press enter to continue...")
    print()


################################
    # 6. Remove Trash
################################

def remove_trash():
    os.chdir(root)
    while True:
        yn = input("Remove Trash folder? (y/n): ")
        if yn == "y" or yn == "yes":
            print("Removing...", end=" ")
            shutil.rmtree(f"{root}/Trash")
            print("Trash folder removed.")
            input("Press enter to continue...")
            print()
            break
        elif yn == "n" or yn == "no":
            print("Trash folder not removed.")
            input("Press enter to continue...")
            print()
            break
        else:
            print(f"{yn} is not a valid option, try again")
            print()


def main():
    download_files()
    extract_files()
    remove_redundant()
    convert_files()
    rename_dta()
    remove_trash()


main()
