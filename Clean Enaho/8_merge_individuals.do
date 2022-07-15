*Individual datasets
**********************

use "$ccc_root/Trash/data_200.dta", clear
merge 1:1 year conglome vivienda hogar codperso using "$ccc_root/Trash/data_300.dta",   nogen
merge 1:1 year conglome vivienda hogar codperso using "$ccc_root/Trash/data_500.dta",   nogen

merge m:1 year conglome vivienda hogar using "$ccc_root/Trash/data_house.dta", nogen
sort  year conglome vivienda hogar codperso
order year conglome vivienda hogar, first
save "$ccc_root/Trash/data_person.dta", replace

