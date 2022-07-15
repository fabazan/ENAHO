use "$ccc_root/Trash/data_sumaria.dta", clear
merge 1:1 year conglome vivienda hogar using "$ccc_root/Trash/data_100.dta", nogen
merge 1:1 year conglome vivienda hogar using "$ccc_root/Trash/data_ethnicity.dta", nogen

save "$ccc_root/Trash/data_house.dta", replace

