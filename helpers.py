import pandas as pd
import dask.dataframe as dd
import sys

def load_file(file):

    # Use the dask.dataframe package to load large CSV files.
    file = dd.read_csv(file, sep=',', encoding='latin-1')

    return file
    
def convert_param_file_data_types(profile_indicator_file, geography_level_file, theme_file):
    
    # Convert column data types to 'int64'.
    profile_indicator_file = profile_indicator_file.fillna(-1)
    profile_indicator_file[["ThemeID", "Sex_Dimension", "MemberId", "MemberParentID", "UOM_ID", "SCALAR_ID", "DECIMALS"]] = \
        profile_indicator_file[
            ["ThemeID", "Sex_Dimension", "MemberId", "MemberParentID", "UOM_ID", "SCALAR_ID", "DECIMALS"]] \
            .astype('int64')

    # Convert column data types to 'str'.
    profile_indicator_file[["ThemeName_EN", "MemberName_EN", "UOM", "SCALAR_FACTOR"]] = \
        profile_indicator_file[["ThemeName_EN", "MemberName_EN", "UOM", "SCALAR_FACTOR"]] \
            .astype('str')

    # Convert column data types to 'int64'.
    geography_level_file[["GEO_LEVEL"]] = geography_level_file[["GEO_LEVEL"]].astype('int64')

    # Convert column data types to 'str'.
    geography_level_file[["Product", "GeographicLevelId", "LevelName_EN"]] = \
        geography_level_file[["Product", "GeographicLevelId", "LevelName_EN"]].astype('str')

    # Convert column data types to 'int64'.
    theme_file[["ThemeID", "Sex_Dimension"]] = theme_file[["ThemeID", "Sex_Dimension"]].astype('int64')

    # Convert column data types to 'str'.
    theme_file[["ThemeName_EN", "ThemeName_FR"]] = theme_file[["ThemeName_EN", "ThemeName_FR"]].astype('str')

    return profile_indicator_file, geography_level_file, theme_file

def members_by_themeID(profile_indicator_file, theme_number):

    # Create a current_members dataframe.
    members_by_themeID.current_members = profile_indicator_file

    # Select the rows with the ThemeID to process.
    members_by_themeID.current_members = \
        members_by_themeID.current_members[members_by_themeID.current_members.ThemeID == int(theme_number)]

    # Remove ThemeID column.
    # members_by_themeID.current_members = members_by_themeID.current_members[["MemberId"]]

    # Change MemberId column to string type.
    members_by_themeID.current_members["MemberId"] = members_by_themeID.current_members["MemberId"].astype('str')

    members_by_themeID.current_members.compute().to_csv("./data/interim/current_members.csv", index=False)

    return members_by_themeID.current_members

def process_da(da_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    da = dd.read_csv(da_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    # Filter source file for GEO_LEVEL equal to 4.
    geo_level_4 = da[da.GEO_LEVEL == 4]

    # Inner join GEO_LEVEL_4 (DA's) Member ID with current_members.csv Member ID.
    geo_level_4["Member ID: Profile of Dissemination Areas (2247)"] = \
        geo_level_4["Member ID: Profile of Dissemination Areas (2247)"].astype('str')

    result = geo_level_4.merge(members_by_themeID.current_members, how='inner',
                               left_on=['Member ID: Profile of Dissemination Areas (2247)'], right_on=['MemberId'])

    result.compute().to_csv("./data/interim/da_joined.csv", index=False)

    # Drop the following columns.
    result = result.drop(
        columns=["MemberId", "CSD_TYPE_NAME", "ALT_GEO_CODE", "Notes: Profile of Dissemination Areas (2247)", "DATA_QUALITY_FLAG"])

    # Rename the following columns.
    result = result.rename(columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                                    "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                                    "DIM: Profile of Dissemination Areas (2247)": "Member",
                                    "Member ID: Profile of Dissemination Areas (2247)": "MemberId",
                                    "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                                    "Dim: Sex (3): Member ID: [2]: Male": "Male",
                                    "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Replace GEO_LEVEL == 4 with 'S0512'.
    result["GEO_LEVEL"] = result["GEO_LEVEL"].replace(4, 'S0512')

    # Create a copy of the REF_DATE column.
    result["REF_DATE_COPY"] = result["REF_DATE"]

    # Convert column data types to 'str'.
    result[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    # result["DGUID"] = result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str').agg("".join, axis=1)
    result["DGUID"] = result["REF_DATE_COPY"] + result["GEO_LEVEL"] + result["GEO_CODE"]

    result = result.drop(
        columns=["REF_DATE_COPY"])

    return result

def process_can_prov_cd_csd(ca_prov_cd_csd_file):

    can_prov_cd_csd = dd.read_csv(ca_prov_cd_csd_file,
                         blocksize="10MB", dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'},
                         low_memory=False)

    can_prov_cd_csd["Member ID: Profile of Census Divisions/Census Subdivisions (2247)"] = \
        can_prov_cd_csd["Member ID: Profile of Census Divisions/Census Subdivisions (2247)"].astype('str')

    result_can_prov_cd_csd = can_prov_cd_csd.merge(members_by_themeID.current_members, how='inner',
                                 left_on=['Member ID: Profile of Census Divisions/Census Subdivisions (2247)'],
                                 right_on=['MemberId'])

    # Drop the following columns.
    result_can_prov_cd_csd = result_can_prov_cd_csd.drop(
        columns=["MemberId", "ALT_GEO_CODE", "CSD_TYPE_NAME", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Census Divisions/Census Subdivisions (2247)"])

    # Replace GEO_LEVEL == .
    result_can_prov_cd_csd["GEO_LEVEL"] = result_can_prov_cd_csd["GEO_LEVEL"].replace(0, 'A0000')
    result_can_prov_cd_csd["GEO_LEVEL"] = result_can_prov_cd_csd["GEO_LEVEL"].replace(1, 'A0002')
    result_can_prov_cd_csd["GEO_LEVEL"] = result_can_prov_cd_csd["GEO_LEVEL"].replace(2, 'A0003')
    result_can_prov_cd_csd["GEO_LEVEL"] = result_can_prov_cd_csd["GEO_LEVEL"].replace(3, 'A0005')
    result_can_prov_cd_csd["GEO_CODE (POR)"] = result_can_prov_cd_csd["GEO_CODE (POR)"].replace('01', '11124')

    # Rename the following columns.
    result_can_prov_cd_csd = result_can_prov_cd_csd.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Census Divisions/Census Subdivisions (2247)": "Member",
                 "Member ID: Profile of Census Divisions/Census Subdivisions (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex" : "Total",
                 "Dim: Sex (3): Member ID: [2]: Male" : "Male",
                 "Dim: Sex (3): Member ID: [3]: Female" : "Female"})

    # Create a copy of the REF_DATE column.
    result_can_prov_cd_csd["REF_DATE_COPY"] = result_can_prov_cd_csd["REF_DATE"]

    # Convert column data types to 'str'.
    result_can_prov_cd_csd[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_can_prov_cd_csd[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    # result["DGUID"] = result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str').agg("".join, axis=1)
    result_can_prov_cd_csd["DGUID"] = result_can_prov_cd_csd["REF_DATE_COPY"] + \
                                      result_can_prov_cd_csd["GEO_LEVEL"] + \
                                      result_can_prov_cd_csd["GEO_CODE"]

    result_can_prov_cd_csd = result_can_prov_cd_csd.drop(
        columns=["REF_DATE_COPY"])

    return result_can_prov_cd_csd

def process_cma_ca(cma_ca_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    cma_ca = dd.read_csv(cma_ca_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    cma_ca["Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)"] = \
        cma_ca["Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)"].astype('str')

    result_cma_ca = cma_ca.merge(members_by_themeID.current_members,
                                 how = 'inner',
                                 left_on = ['Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)'],
                                 right_on=['MemberId'])

    # Drop the following columns.
    result_cma_ca = result_cma_ca.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Census Metropolitan Areas/Census Agglomerations (2247)"])

    # Replace GEO_LEVEL == 2,3.
    result_cma_ca["GEO_LEVEL"] = result_cma_ca["GEO_LEVEL"].replace(2, 'S0503')
    result_cma_ca["GEO_LEVEL"] = result_cma_ca["GEO_LEVEL"].replace(3, 'S0503')

    # Rename the following columns.
    result_cma_ca = result_cma_ca.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Census Metropolitan Areas/Census Agglomerations (2247)": "Member",
                 "Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_cma_ca["REF_DATE_COPY"] = result_cma_ca["REF_DATE"]

    # Convert column data types to 'str'.
    result_cma_ca[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_cma_ca[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_cma_ca["DGUID"] = result_cma_ca["REF_DATE_COPY"] + \
                             result_cma_ca["GEO_LEVEL"] + \
                             result_cma_ca["GEO_CODE"]

    result_cma_ca = result_cma_ca.drop(
        columns=["REF_DATE_COPY"])

    return result_cma_ca

def process_er(er_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    er = dd.read_csv(er_file, blocksize="10MB",
                         dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    # Filter source file for GEO_LEVEL equal to 2.
    er = er[er.GEO_LEVEL == 2]

    er["Member ID: Profile of Economic Regions (2247)"] = \
        er["Member ID: Profile of Economic Regions (2247)"].astype('str')

    result_er = er.merge(members_by_themeID.current_members,
                                 how='inner',
                                 left_on=['Member ID: Profile of Economic Regions (2247)'],
                                 right_on=['MemberId'])

    # Drop the following columns.
    result_er = result_er.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Economic Regions (2247)"])

    # Replace GEO_LEVEL == 2.
    result_er["GEO_LEVEL"] = result_er["GEO_LEVEL"].replace(2, 'S0500')

    # Rename the following columns.
    result_er = result_er.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Economic Regions (2247": "Member",
                 "Member ID: Profile of Economic Regions (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_er["REF_DATE_COPY"] = result_er["REF_DATE"]

    # Convert column data types to 'str'.
    result_er[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_er[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_er["DGUID"] = result_er["REF_DATE_COPY"] + \
                             result_er["GEO_LEVEL"] + \
                             result_er["GEO_CODE"]

    return result_er

def process_pc(pc_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    pc = dd.read_csv(pc_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    pc["Member ID: Profile of Population Centres (2247)"] = \
        pc["Member ID: Profile of Population Centres (2247)"].astype('str')

    result_pc = pc.merge(members_by_themeID.current_members,
                         how='inner',
                         left_on=['Member ID: Profile of Population Centres (2247)'],
                         right_on=['MemberId'])

    # Drop the following columns.
    result_pc = result_pc.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Population Centres (2247)"])

    # Replace GEO_LEVEL == 1.
    result_pc["GEO_LEVEL"] = result_pc["GEO_LEVEL"].replace(1, 'S0510')

    # Rename the following columns.
    result_pc = result_pc.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Population Centres (2247)": "Member",
                 "Member ID: Profile of Population Centres (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_pc["REF_DATE_COPY"] = result_pc["REF_DATE"]

    # Convert column data types to 'str'.
    result_pc[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_pc[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_pc["DGUID"] = result_pc["REF_DATE_COPY"] + \
                         result_pc["GEO_LEVEL"] + \
                         result_pc["GEO_CODE"]

    return result_pc

def process_hr(hr_file):
    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    hr = dd.read_csv(hr_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    # Filter source file for GEO_LEVEL equal to 2.
    hr = hr[hr.GEO_LEVEL == 2]

    hr["Member ID: Profile of Health Regions (2247)"] = \
        hr["Member ID: Profile of Health Regions (2247)"].astype('str')

    result_hr = hr.merge(members_by_themeID.current_members,
                         how='inner',
                         left_on=['Member ID: Profile of Health Regions (2247)'],
                         right_on=['MemberId'])

    # Drop the following columns.
    result_hr = result_hr.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Health Regions (2247)"])

    # Replace GEO_LEVEL == 1.
    result_hr["GEO_LEVEL"] = result_hr["GEO_LEVEL"].replace(2, 'A0007')

    # Rename the following columns.
    result_hr = result_hr.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Health Regions (2247)": "Member",
                 "Member ID: Profile of Health Regions (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_hr["REF_DATE_COPY"] = result_hr["REF_DATE"]

    # Convert column data types to 'str'.
    result_hr[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_hr[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_hr["DGUID"] = result_hr["REF_DATE_COPY"] + \
                         result_hr["GEO_LEVEL"] + \
                         result_hr["GEO_CODE"]

    return result_hr

def process_dp(dp_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    dp = dd.read_csv(dp_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    dp["Member ID: Profile of Designated Places (2247)"] = \
        dp["Member ID: Profile of Designated Places (2247)"].astype('str')

    result_dp = dp.merge(members_by_themeID.current_members,
                         how='inner',
                         left_on=['Member ID: Profile of Designated Places (2247)'],
                         right_on=['MemberId'])

    # Drop the following columns.
    result_dp = result_dp.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Designated Places (2247)"])

    # Replace GEO_LEVEL == 1.
    result_dp["GEO_LEVEL"] = result_dp["GEO_LEVEL"].replace(1, 'A0006')

    # Rename the following columns.
    result_dp = result_dp.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Designated Places (2247)": "Member",
                 "Member ID: Profile of Designated Places (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_dp["REF_DATE_COPY"] = result_dp["REF_DATE"]

    # Convert column data types to 'str'.
    result_dp[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_dp[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_dp["DGUID"] = result_dp["REF_DATE_COPY"] + \
                         result_dp["GEO_LEVEL"] + \
                         result_dp["GEO_CODE"]

    return result_dp

def process_fed(fed_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    fed = dd.read_csv(fed_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object'}, low_memory=False)

    # Filter source file for GEO_LEVEL equal to 2.
    fed = fed[fed.GEO_LEVEL == 2]

    fed["Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)"] = \
        fed["Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)"].astype('str')

    result_fed = fed.merge(members_by_themeID.current_members,
                         how='inner',
                         left_on=['Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)'],
                         right_on=['MemberId'])

    # Drop the following columns.
    result_fed = result_fed.drop(
        columns=["MemberId", "ALT_GEO_CODE", "DATA_QUALITY_FLAG",
                 "Notes: Profile of Federal Electoral Districts (2013 Representation Order) (2247)"])


    # Replace GEO_LEVEL == 1.
    result_fed["GEO_LEVEL"] = result_fed["GEO_LEVEL"].replace(1, 'A0006')

    # Rename the following columns.
    result_fed = result_fed.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Federal Electoral Districts (2013 Representation Order) (2247)": "Member",
                 "Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_fed["REF_DATE_COPY"] = result_fed["REF_DATE"]

    # Convert column data types to 'str'.
    result_fed[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_fed[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_fed["DGUID"] = result_fed["REF_DATE_COPY"] + \
                         result_fed["GEO_LEVEL"] + \
                         result_fed["GEO_CODE"]

    return result_fed

def process_fsa(fsa_file):

    # Read the source test file.
    # The dask.dataframe package may be needed for reading larger CSV files.
    fsa = dd.read_csv(fsa_file, blocksize="10MB",
                     dtype={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'object',
                            'ALT_GEO_CODE': 'object',
                            'GEO_CODE (POR)': 'object'}, low_memory=False)

    # Filter source file for GEO_LEVEL equal to 2.
    fsa = fsa[fsa.GEO_LEVEL == 2]

    fsa["Member ID: Profile of Forward Sortation Areas (2247)"] = \
        fsa["Member ID: Profile of Forward Sortation Areas (2247)"].astype('str')

    result_fsa = fsa.merge(members_by_themeID.current_members,
                         how='inner',
                         left_on=['Member ID: Profile of Forward Sortation Areas (2247)'],
                         right_on=['MemberId'])
    
    # Drop the following columns.
    result_fsa = result_fsa.drop(
        columns=["MemberId", "ALT_GEO_CODE", "Notes: Profile of Forward Sortation Areas (2247)", "DATA_QUALITY_FLAG"])
    
    # Replace GEO_LEVEL == 1.
    result_fsa["GEO_LEVEL"] = result_fsa["GEO_LEVEL"].replace(2, 'A0011')

    # Rename the following columns.
    result_fsa = result_fsa.rename(
        columns={"CENSUS_YEAR": "REF_DATE", "GEO_CODE (POR)": "GEO_CODE", "GEO_NAME": "GEO",
                 "GNR": "Short form: Non-response", "GNR_LF": "Long form: Non-response",
                 "DIM: Profile of Forward Sortation Areas (2247)": "Member",
                 "Member ID: Profile of Forward Sortation Areas (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_fsa["REF_DATE_COPY"] = result_fsa["REF_DATE"]

    # Convert column data types to 'str'.
    result_fsa[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]] = \
        result_fsa[["REF_DATE_COPY", "GEO_LEVEL", "GEO_CODE"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_fsa["DGUID"] = result_fsa["REF_DATE_COPY"] + \
                         result_fsa["GEO_LEVEL"] + \
                         result_fsa["GEO_CODE"]
    
    return result_fsa

def process_product_en(combine, profile_indicator_file):

    combine = combine.drop(columns=["Short form: Non-response", "Long form: Non-response"])
        
    profile_indicator_file["MemberId"] = profile_indicator_file["MemberId"].astype('str')

    joined = combine.merge(profile_indicator_file, how='left',
                               left_on=['MemberId'], right_on=['MemberId'])

    ### ExpandTableColumn

    renamed = joined.rename(columns={"ThemeMember": "ThemeMemberId2"})

    unpivot = dd.melt(
        renamed, 
        value_vars = ["Total", "Male", "Female"],
        id_vars = ["REF_DATE", "GEO_CODE", "GEO_LEVEL","GEO", "Member","MemberId", "DGUID", "ThemeID", 
                   "Sex_Dimension", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "DECIMALS", "ThemeMemberId2"],
        var_name="Sex",
        value_name="Indicator")

    unpivot["STATUS"] = ""

    unpivot["Indicator"] = unpivot["Indicator"].replace('...', '')
    unpivot["Indicator"] = unpivot["Indicator"].replace('..', '')
    unpivot["Indicator"] = unpivot["Indicator"].replace('F', '')
    unpivot["Indicator"] = unpivot["Indicator"].replace('x', '')

    unpivot["VECTOR_Index"] = ''
    unpivot = unpivot.assign(VECTOR_Index = unpivot.reset_index().index + 98401000100)

    unpivot["VECTOR_Index"] = unpivot["VECTOR_Index"].astype('str')

    unpivot["VECTOR"] = "v" + unpivot["VECTOR_Index"]

    unpivot = unpivot.drop(columns=["VECTOR_Index"])

    # Rename the following columns.
    unpivot = unpivot.rename(columns={"Indicator": "Value"})

    sexid = lambda x: "1" if x == "Total" else "2" if x == "Male" else "3" if x == "Female" else ""
    unpivot = unpivot.assign(SexId = unpivot.Sex.apply(sexid, meta=('Sex', 'object')))

    unpivot["SYMBOL"] = ''
    unpivot["TERMINATED"] = ''

    unpivot = unpivot[["REF_DATE", "GEO", "DGUID", "Member", "Sex", "UOM", 
                       "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", "Value", 
                       "STATUS", "SYMBOL", "TERMINATED", "DECIMALS", "ThemeID", 
                       "Sex_Dimension", "ThemeMemberId2", "SexId"]]

    return unpivot

def process_product_en_no_sex(product_en):

    product_en_no_sex = product_en[product_en.Sex_Dimension == 0]

    product_en_no_sex = product_en_no_sex[product_en_no_sex.Sex == 'Total']

    product_en_no_sex["ThemeMemberId2"] = product_en_no_sex["ThemeMemberId2"].astype('str')

    pd_product_en_no_sex = product_en_no_sex.compute()

    pd_product_en_no_sex['TEMP_COORD'] = pd.factorize(pd_product_en_no_sex.GEO)[0] + 1

    product_en_no_sex = dd.from_pandas(pd_product_en_no_sex, npartitions=2)

    product_en_no_sex["TEMP_COORD"] = product_en_no_sex["TEMP_COORD"].astype('str')

    product_en_no_sex["COORDINATE"] = product_en_no_sex["TEMP_COORD"] + "." + product_en_no_sex["ThemeMemberId2"]

    product_en_no_sex = product_en_no_sex.drop(columns = ["ThemeMemberId2"])

    product_en_no_sex = product_en_no_sex[["REF_DATE", "DGUID", "GEO", "Member", "Sex", "UOM", 
                                           "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", 
                                           "COORDINATE", "Value", "STATUS", "DECIMALS"]]

    return product_en_no_sex

def process_product_en_sex(product_en):

    product_en_sex = product_en[product_en.Sex_Dimension == 1]

    product_en_sex["ThemeMemberId2"] = product_en_sex["ThemeMemberId2"].astype('str')

    product_en_sex["SexId"] = product_en_sex["SexId"].astype('str')

    pd_product_en_sex = product_en_sex.compute()

    pd_product_en_sex['TEMP_COORD'] = pd.factorize(pd_product_en_sex.GEO)[0] + 1

    product_en_sex = dd.from_pandas(pd_product_en_sex, npartitions=2)

    product_en_sex["TEMP_COORD"] = product_en_sex["TEMP_COORD"].astype('str')

    product_en_sex["COORDINATE"] = product_en_sex["TEMP_COORD"] + "." + product_en_sex["ThemeMemberId2"] + "." + product_en_sex["SexId"]

    product_en_sex = product_en_sex.drop(columns = ["ThemeMemberId2"])

    product_en_sex = product_en_sex[["REF_DATE", "DGUID", "GEO", "Member", "Sex", "UOM", 
                                     "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", 
                                     "COORDINATE", "Value", "STATUS", "DECIMALS"]]

    return product_en_sex