import json
import os
from pathlib import Path
from subprocess import Popen
from typing import Dict

import pandas as pd

mdk_config = {
    "analysis_settings_json": "analysis_settings.json",
    "lookup_data_dir": "keys_data",
    "lookup_module_path": "src/keys_server/ParisWindstormKeysLookup.py",
    "model_data_dir": "model_data",
    "model_version_csv": "keys_data/ModelVersion.csv",
    "oed_accounts_csv": "tests/account.csv",
    "oed_location_csv": "tests/location.csv",
    "hashed_group_id": True
}


def get_recent_run_path() -> str:
    path_string: str = str(Path.cwd()) + "/runs/"
    path = Path(path_string)

    for directory in [str(f) for f in path.iterdir() if f.is_dir()]:
        if "losses" in directory:
            return directory


def rename_directory(directory_path: str, new_file_name: str) -> str:
    buffer = directory_path.split("/")
    buffer[-1] = new_file_name
    new_path: str = "/".join(buffer)
    os.rename(directory_path, new_path)
    return new_path


def get_output_files(run_directory: str) -> Dict[str, str]:
    output_path: str = run_directory + "/output/"
    output = dict()
    output["gul_S1_aalcalc.csv"] = output_path + "gul_S1_aalcalc.csv"
    output["gul_S1_eltcalc.csv"] = output_path + "gul_S1_eltcalc.csv"
    output["gul_S1_leccalc_full_uncertainty_aep.csv"] = output_path + "gul_S1_leccalc_full_uncertainty_aep.csv"
    output["gul_S1_leccalc_full_uncertainty_oep.csv"] = output_path + "gul_S1_leccalc_full_uncertainty_oep.csv"
    output["gul_S1_summary-info.csv"] = output_path + "gul_S1_summary-info.csv"
    output["il_S1_aalcalc.csv"] = output_path + "il_S1_aalcalc.csv"
    output["il_S1_eltcalc.csv"] = output_path + "il_S1_eltcalc.csv"
    output["il_S1_leccalc_full_uncertainty_aep.csv"] = output_path + "il_S1_leccalc_full_uncertainty_aep.csv"
    output["il_S1_leccalc_full_uncertainty_oep.csv"] = output_path + "il_S1_leccalc_full_uncertainty_oep.csv"
    output["il_S1_summary-info.csv"] = output_path + "il_S1_summary-info.csv"
    return output


def compare_data(hash_output_dict: Dict[str, str], none_hash_output_dict: Dict[str, str], key: str) -> None:

    hash_data_path: str = hash_output_dict[key]
    none_hash_data_path: str = none_hash_output_dict[key]

    hash_df = pd.read_csv(hash_data_path, index_col=False)
    non_hash_df = pd.read_csv(none_hash_data_path, index_col=False)

    difference = pd.concat([hash_df, non_hash_df]).drop_duplicates(keep=False)

    print(f"left number is {len(hash_df)} right number is {len(non_hash_df)}")
    if len(difference) > 0:
        print(f"the difference between hash and none hash for {key} is {len(difference)}")
        print(difference.head())
    else:
        print(f"there is no difference between hash and none hash for {key}")


def generate_location_data(remove_location: bool = False) -> pd.DataFrame:
    data = [
        [1,1,1,"Hotel Ronceray Opera",48.874979,2.30887,5150,1000000,0,0,0,"WTC","EUR","FR",10000,500000],
        [1,1,2,"Gare Du Nord",48.876918,2.324729,5050,2000000,0,0,0,"WTC","EUR","FR",25000,1000000],
        [1,1,3,"Art Supply Store",48.85324,2.387931,5150,500000,0,0,0,"WTC","EUR","FR",0,0]
    ]
    if remove_location is True:
        data = data[1:]
    columns = [
        "PortNumber","AccNumber","LocNumber","LocName","Latitude","Longitude","ConstructionCode","BuildingTIV",
        "OtherTIV","ContentsTIV","BITIV","LocPerilsCovered","LocCurrency","CountryCode","LocDed6All","LocLimit6All"
    ]
    df = pd.DataFrame(data, columns=columns)
    return df


if __name__ == "__main__":
    # cleanup the previous runs
    main_path: str = str(Path.cwd())
    remove_runs = Popen(f"rm -r ./runs/", shell=True)
    remove_runs.wait()

    # setup the datasets for locations
    # "oed_location_csv": "tests/location.csv"
    locations = generate_location_data()
    reduced_locations = generate_location_data(remove_location=True)

    # write the location data
    locations.to_csv("./tests/full_locations.csv", index=False)
    reduced_locations.to_csv("./tests/reduced_locations.csv", index=False)
    mdk_config["oed_location_csv"] = "tests/full_locations.csv"

    # update the local oasislmf pip module
    update_oasislmf = Popen("screw-update-local-oasislmf")
    update_oasislmf.wait()

    # write the new MDK config
    with open(f"./hash_test_mdk.json", "w") as file:
        file.write(json.dumps(mdk_config))

    run_model = Popen(f"oasislmf model run --config ./hash_test_mdk.json", shell=True)
    run_model.wait()

    hash_run_path: str = rename_directory(directory_path=get_recent_run_path(), new_file_name="full_location_run")

    mdk_config["oed_location_csv"] = "tests/reduced_locations.csv"

    with open(f"./none_hash_test_mdk.json", "w") as file:
        file.write(json.dumps(mdk_config))

    run_model = Popen(f"oasislmf model run --config ./none_hash_test_mdk.json", shell=True)
    run_model.wait()

    none_hash_run_path: str = rename_directory(directory_path=get_recent_run_path(), new_file_name="reduced_locations_run")

    hashed_outputs = get_output_files(run_directory=hash_run_path)
    none_hashed_outputs = get_output_files(run_directory=none_hash_run_path)

    for key in hashed_outputs.keys():
        compare_data(hash_output_dict=hashed_outputs, none_hash_output_dict=none_hashed_outputs, key=key)

    os.remove(f"./hash_test_mdk.json")
    os.remove(f"./none_hash_test_mdk.json")
