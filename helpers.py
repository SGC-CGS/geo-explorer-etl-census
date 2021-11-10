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
    members_by_themeID.current_members = members_by_themeID.current_members[["MemberId"]]

    # Change MemberId column to string type.
    members_by_themeID.current_members["MemberId"] = members_by_themeID.current_members["MemberId"].astype('str')

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

    # Drop the following columns.
    result = result.drop(
        columns=["CSD_TYPE_NAME", "ALT_GEO_CODE", "Notes: Profile of Dissemination Areas (2247)", "DATA_QUALITY_FLAG"])

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
    result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    # result["DGUID"] = result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str').agg("".join, axis=1)
    result["DGUID"] = result["REF_DATE_COPY"] + result["GEO_LEVEL"] + result["GEO"]

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
    result_ca_prov_cd_csd = result_can_prov_cd_csd.drop(
        columns=["ALT_GEO_CODE", "CSD_TYPE_NAME", "DATA_QUALITY_FLAG",
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
                 "Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)": "MemberId",
                 "Dim: Sex (3): Member ID: [1]: Total - Sex": "Total",
                 "Dim: Sex (3): Member ID: [2]: Male": "Male",
                 "Dim: Sex (3): Member ID: [3]: Female": "Female"})

    # Create a copy of the REF_DATE column.
    result_can_prov_cd_csd["REF_DATE_COPY"] = result_can_prov_cd_csd["REF_DATE"]

    # Convert column data types to 'str'.
    result_can_prov_cd_csd[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_can_prov_cd_csd[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    # result["DGUID"] = result[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str').agg("".join, axis=1)
    result_can_prov_cd_csd["DGUID"] = result_can_prov_cd_csd["REF_DATE_COPY"] + \
                                      result_can_prov_cd_csd["GEO_LEVEL"] + \
                                      result_can_prov_cd_csd["GEO"]

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
    result_cma_ca[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_cma_ca[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_cma_ca["DGUID"] = result_cma_ca["REF_DATE_COPY"] + \
                             result_cma_ca["GEO_LEVEL"] + \
                             result_cma_ca["GEO"]

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
    result_er[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_er[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_er["DGUID"] = result_er["REF_DATE_COPY"] + \
                             result_er["GEO_LEVEL"] + \
                             result_er["GEO"]

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
    result_pc[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_pc[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_pc["DGUID"] = result_pc["REF_DATE_COPY"] + \
                         result_pc["GEO_LEVEL"] + \
                         result_pc["GEO"]

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
    result_hr[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_hr[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_hr["DGUID"] = result_hr["REF_DATE_COPY"] + \
                         result_hr["GEO_LEVEL"] + \
                         result_hr["GEO"]

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
    result_dp[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_dp[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_dp["DGUID"] = result_dp["REF_DATE_COPY"] + \
                         result_dp["GEO_LEVEL"] + \
                         result_dp["GEO"]

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
    result_fed[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]] = \
        result_fed[["REF_DATE_COPY", "GEO_LEVEL", "GEO"]].astype('str')

    # Create a new column called "DGUID" based on combining the following columns.
    result_fed["DGUID"] = result_fed["REF_DATE_COPY"] + \
                         result_fed["GEO_LEVEL"] + \
                         result_fed["GEO"]

    return result_fed