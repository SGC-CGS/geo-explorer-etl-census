import dask.dataframe as dd
from dask.array import from_array as fa
import helpers
import logging
import sys

# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)

class Creator:

    def load_prerequisite_params(self):

        logger.info("Loading prerequisite parameter files.")

        # Read the profile_indicators.csv into a dataframe.
        self.profile_indicators = helpers.load_file('./params/profile_indicators.csv')

        # Read the geography_level.csv into a dataframe.
        self.geography_level = helpers.load_file('./params/geography_level.csv')

        # Read the theme.csv into a dataframe.
        self.theme = helpers.load_file('./params/theme.csv')

        logger.info("Converting prerequisite parameter file column data types.")

        # Convert prerequisite parameter file data types.
        self.profile_indicators, self.geography_level, self.theme = \
        helpers.convert_param_file_data_types(self.profile_indicators, self.geography_level, self.theme)


    def generate_members_by_themeID(self):

        logger.info("Generate members by Theme ID.")

        self.current_members = helpers.members_by_themeID(self.profile_indicators, 11)


    def generate_da(self):

        logger.info("Generate Dissemination Area (DA) file.")

        self.da = helpers.process_da(
            './data/raw/census/98-401-X2016044_eng_CSV/98-401-X2016044_English_CSV_data.csv')

        # self.da.compute().to_csv("./data/processed/da.csv", index=False)


    def generate_can_prov_cd_csd(self):

        logger.info("Generate Canada (CAN), Province (PR), Census Division (CD), Census Subdivision (CSD) file.")

        self.can_prov_cd_csd = \
            helpers.process_can_prov_cd_csd(
                './data/raw/census/98-401-X2016055_eng_CSV/98-401-X2016055_English_CSV_data.csv')

        # self.ca_prov_cd_csd.compute().to_csv("./data/processed/can_prov_cd_csd.csv", index=False)

    def generate_cma_ca(self):

        logger.info("Generate Census Metropolitan Areas (CMA)/Census Agglomerations (CA) file.")

        self.cma_ca = \
            helpers.process_cma_ca(
                './data/raw/census/98-401-X2016041_eng_CSV/98-401-X2016041_English_CSV_data.csv')

        # self.cma_ca.compute().to_csv("./data/processed/cma_ca.csv", index=False)

    def generate_er(self):

        logger.info("Generate Economic Regions (ER) file.")

        self.er = \
            helpers.process_er(
                './data/raw/census/98-401-X2016049_eng_CSV/98-401-X2016049_English_CSV_data.csv')

        # self.er.compute().to_csv("./data/processed/er.csv", index=False)

    def generate_pc(self):

        logger.info("Generate Population Centres (PC) file.")

        self.pc = \
            helpers.process_pc(
                './data/raw/census/98-401-X2016048_eng_CSV/98-401-X2016048_English_CSV_data.csv')

        # self.pc.compute().to_csv("./data/processed/pc.csv", index=False)

    def generate_hr(self):

        logger.info("Generate Health Regions (HR) file.")

        self.hr = \
            helpers.process_hr(
                './data/raw/census/98-401-X2016058_eng_CSV/98-401-X2016058_English_CSV_data.csv')

        # self.hr.compute().to_csv("./data/processed/hr.csv", index=False)

    def generate_dp(self):

        logger.info("Generate Designated Places (DP) file.")

        self.dp = \
            helpers.process_dp(
                './data/raw/census/98-401-X2016047_eng_CSV/98-401-X2016047_English_CSV_data.csv')

        # self.dp.compute().to_csv("./data/processed/dp.csv", index=False)

    def generate_fed(self):

        logger.info("Generate Federal Electoral Districts (FED) file.")

        self.fed = \
            helpers.process_fed(
                './data/raw/census/98-401-X2016045_eng_CSV/98-401-X2016045_English_CSV_data.csv')

        # self.fed.compute().to_csv("./data/processed/fed.csv", index=False)

    def generate_fsa(self):

        logger.info("Generate Forward Sortation Area (FSA) file.")

        self.fsa = \
            helpers.process_fsa(
                './data/raw/census/98-401-X2016046_eng_CSV/98-401-X2016046_English_CSV_data.csv')

        # self.fsa.compute().to_csv("./data/processed/fsa.csv", index=False)

    def generate_product_en(self):

        logger.info("Generate Product (EN).")

        # Combine can_prov_csd_cd, cma_ca, da.
        combine = self.can_prov_cd_csd.append(self.cma_ca, self.da)

        combine = combine.drop(columns=["Short form: Non-response", "Long form: Non-response"])
        
        self.profile_indicators["MemberId"] = self.profile_indicators["MemberId"].astype('str')

        joined = combine.merge(self.profile_indicators, how='left',
                               left_on=['MemberId'], right_on=['MemberId'])

        ### ExpandTableColumn

        # renamed = joined.rename(columns={"ThemeMember": "ThemeMemberId"})
        

        unpivot = dd.melt(
            joined, 
            value_vars = ["Total", "Male", "Female"],
            id_vars = ["REF_DATE", "GEO_CODE", "GEO_LEVEL","GEO", "Member","MemberId", "DGUID", "ThemeID", 
                       "Sex_Dimension", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "DECIMALS", "ThemeMember", "ThemeMemberId"],
            var_name="Sex",
            value_name="Indicator")

        unpivot["STATUS"] = ""

        unpivot["Indicator"] = unpivot["Indicator"].replace('...', '')
        unpivot["Indicator"] = unpivot["Indicator"].replace('..', '')
        unpivot["Indicator"] = unpivot["Indicator"].replace('F', '')
        unpivot["Indicator"] = unpivot["Indicator"].replace('x', '')

        unpivot["VECTOR_Index"] = ''
        unpivot = unpivot.assign(VECTOR_Index = unpivot.reset_index().index + 98401000100)

        unpivot.compute().to_csv("./data/processed/unpivot.csv", index=False)
        # joined.compute().to_csv("./data/processed/joined.csv", index=False)




    def execute(self):

        self.load_prerequisite_params()
        self.generate_members_by_themeID()
        self.generate_da()
        self.generate_can_prov_cd_csd()
        self.generate_cma_ca()
        self.generate_er()
        self.generate_pc()
        self.generate_hr()
        self.generate_dp()
        self.generate_fed()
        self.generate_fsa()
        self.generate_product_en()



def main():
    creator = Creator()
    creator.execute()

if __name__ == "__main__":
    try:

        main()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)