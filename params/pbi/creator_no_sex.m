section Section1;

shared DA_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016044_eng_CSV\98-401-X2016044_English_CSV_data.csv"),[Delimiter=",", Columns=15, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Filtered Rows" = Table.SelectRows(#"Promoted Headers", each [GEO_LEVEL] = "4"),
    #"Merged Queries" = Table.NestedJoin(#"Filtered Rows", {"Member ID: Profile of Dissemination Areas (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"CSD_TYPE_NAME", "ALT_GEO_CODE", "Current_Members", "Notes: Profile of Dissemination Areas (2247)", "DATA_QUALITY_FLAG"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Dissemination Areas (2247)", "Member"}, {"Member ID: Profile of Dissemination Areas (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","4","S0512",Replacer.ReplaceText,{"GEO_LEVEL"}),
    Custom1 = Table.DuplicateColumn(#"Replaced Value", "REF_DATE", "REF_DATE - Copy"),
    Custom2 = Table.CombineColumns(Custom1,{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE" },Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    Custom2;

shared Profile_Indicators = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Parameters\Profile_Indicators.csv"),[Delimiter=",", Columns=13, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"ThemeID", Int64.Type}, {"ThemeName_EN", type text}, {"Sex_Dimension", Int64.Type}, {"MemberId", Int64.Type}, {"MemberParentID", Int64.Type}, {"MemberName_EN", type text}, {"UOM", type text}, {"UOM_ID", Int64.Type}, {"SCALAR_FACTOR", type text}, {"SCALAR_ID", Int64.Type}, {"DECIMALS", Int64.Type}})
in
    #"Changed Type";

shared Theme = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Parameters\Theme.csv"),[Delimiter=",", Columns=4, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"ThemeID", Int64.Type}, {"ThemeName_EN", type text}, {"ThemeName_FR", type text}, {"Sex_Dimension", Int64.Type}})
in
    #"Changed Type";

shared Product_EN = let
    Source = CMA_CA_EN,
    #"Appended Query" = Table.Combine({Source, HR_EN, CAN_PROV_CD_CSD_EN, DA_EN, ER_EN}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Appended Query",{{"MemberId", type number}}),
    #"Removed Columns" = Table.RemoveColumns(#"Changed Type1",{"Short form: Non-response", "Long Form: Non-response", "Long form: Non-response", "Male", "Female"}),
    #"Merged Queries1" = Table.NestedJoin(#"Removed Columns", {"MemberId"}, Profile_Indicators, {"MemberId"}, "Profile_Indicators", JoinKind.LeftOuter),
    #"Expanded Profile_Indicators" = Table.ExpandTableColumn(#"Merged Queries1", "Profile_Indicators", {"ThemeID", "Sex_Dimension", "ThemeMember", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "DECIMALS"}, {"ThemeID", "Sex_Dimension", "ThemeMember", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "DECIMALS"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Expanded Profile_Indicators",{{"ThemeMember", "ThemeMemberId"}, {"Total", "Indicator"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns1", each [ThemeID] = Theme_to_Process),
    #"Added Conditional Column" = Table.AddColumn(#"Filtered Rows", "STATUS", each if [Indicator] = ".." then [Indicator] else if [Indicator] = "..." then [Indicator] else if [Indicator] = "F" then [Indicator] else if [Indicator] = "x" then [Indicator] else "" ),
    #"Replaced Value1" = Table.ReplaceValue(#"Added Conditional Column","...","",Replacer.ReplaceText,{"Indicator"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","..","",Replacer.ReplaceText,{"Indicator"}),
    #"Replaced Value3" = Table.ReplaceValue(#"Replaced Value2","F","",Replacer.ReplaceText,{"Indicator"}),
    #"Replaced Value4" = Table.ReplaceValue(#"Replaced Value3","x","",Replacer.ReplaceText,{"Indicator"}),
    #"Added Index" = Table.AddIndexColumn(#"Replaced Value4", "VECTOR_Index", 98401000100, 1),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Index",{{"VECTOR_Index", type text}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type3", "VECTOR", each "v"&[VECTOR_Index]),
    #"Removed Columns3" = Table.RemoveColumns(#"Added Custom1",{"VECTOR_Index", "ThemeID"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns3",{{"Indicator", "Value"}}),
    #"Reordered Columns" = Table.ReorderColumns(#"Renamed Columns",{"REF_DATE", "DGUID", "GEO", "Member", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "Value", "STATUS", "DECIMALS", "MemberId"}),
    #"Reordered Columns1" = Table.ReorderColumns(#"Reordered Columns",{"REF_DATE", "DGUID", "GEO", "Member", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", "Value", "STATUS", "DECIMALS", "ThemeMemberId"}),
    #"Added Custom2" = Table.AddColumn(#"Reordered Columns1", "SYMBOL", each ""),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "TERMINATED", each ""),
    #"Reordered Columns2" = Table.ReorderColumns(#"Added Custom3",{"REF_DATE", "GEO", "DGUID", "Member", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", "Value", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS", "ThemeMemberId"}),
    Custom1 = Table.AddColumn(#"Reordered Columns2", "COORDINATE", each "1."&[ThemeMemberId]),
    Custom2 = Table.RemoveColumns(#"Custom1",{"ThemeMemberId", "Sex_Dimension"}),
    Custom3 = Table.ReorderColumns(#"Custom2",{"REF_DATE", "GEO", "DGUID", "Member", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR", "COORDINATE", "Value", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"})
in
    Custom3;

shared CAN_PROV_CD_CSD_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016055_eng_CSV\98-401-X2016055_English_CSV_data.csv"),[Delimiter=",", Columns=15, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Queries" = Table.NestedJoin(#"Promoted Headers", {"Member ID: Profile of Census Divisions/Census Subdivisions (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns1" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "CSD_TYPE_NAME", "DATA_QUALITY_FLAG", "Notes: Profile of Census Divisions/Census Subdivisions (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns1","0","A0000",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","1","A0002",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","2","A0003",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Replaced Value3" = Table.ReplaceValue(#"Replaced Value2","3","A0005",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Replaced Value4" = Table.ReplaceValue(#"Replaced Value3","01","11124",Replacer.ReplaceValue,{"GEO_CODE (POR)"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value4",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"DIM: Profile of Census Divisions/Census Subdivisions (2247)", "Member"}, {"Member ID: Profile of Census Divisions/Census Subdivisions (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}, {"GNR_LF", "Long form: Non-response"}, {"GNR", "Short form: Non-response"}, {"GEO_NAME", "GEO"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE" },Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared CMA_CA_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016041_eng_CSV\98-401-X2016041_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Queries" = Table.NestedJoin(#"Promoted Headers", {"Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Census Metropolitan Areas/Census Agglomerations (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","2","S0503",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","3","S0503",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value1",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"DIM: Profile of Census Metropolitan Areas/Census Agglomerations (2247)", "Member"}, {"Member ID: Profile of Census Metropolitan Areas/Census Agglomerations (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID"),
    #"Filtered Rows" = Table.SelectRows(#"Merged Columns", each true)
in
    #"Filtered Rows";

shared ER_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016049_eng_CSV\98-401-X2016049_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Filtered Rows1" = Table.SelectRows(#"Promoted Headers", each ([GEO_LEVEL] = "2")),
    #"Merged Queries" = Table.NestedJoin(#"Filtered Rows1", {"Member ID: Profile of Economic Regions (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Economic Regions (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","2","S0500",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Economic Regions (2247)", "Member"}, {"Member ID: Profile of Economic Regions (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared PC_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016048_eng_CSV\98-401-X2016048_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Queries" = Table.NestedJoin(#"Promoted Headers", {"Member ID: Profile of Population Centres (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Population Centres (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","1","S0510",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Population Centres (2247)", "Member"}, {"Member ID: Profile of Population Centres (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared HR_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016058_eng_CSV\98-401-X2016058_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Filtered Rows" = Table.SelectRows(#"Promoted Headers", each ([GEO_LEVEL] = "2")),
    #"Merged Queries" = Table.NestedJoin(#"Filtered Rows", {"Member ID: Profile of Health Regions (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Health Regions (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","2","A0007",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long Form: Non-response"}, {"DIM: Profile of Health Regions (2247)", "Member"}, {"Member ID: Profile of Health Regions (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared DP_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016047_eng_CSV\98-401-X2016047_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Merged Queries" = Table.NestedJoin(#"Promoted Headers", {"Member ID: Profile of Designated Places (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Designated Places (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","1","A0006",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Designated Places (2247)", "Member"}, {"Member ID: Profile of Designated Places (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared FED_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016045_eng_CSV\98-401-X2016045_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Filtered Rows" = Table.SelectRows(#"Promoted Headers", each ([GEO_LEVEL] = "2")),
    #"Merged Queries" = Table.NestedJoin(#"Filtered Rows", {"Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.Inner),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Federal Electoral Districts (2013 Representation Order) (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","2","A0004",Replacer.ReplaceValue,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Federal Electoral Districts (2013 Representation Order) (2247)", "Member"}, {"Member ID: Profile of Federal Electoral Districts (2013 Representation Order) (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared FSA_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\DATA_SOURCES\98-401-X2016046_eng_CSV\98-401-X2016046_English_CSV_data.csv"),[Delimiter=",", Columns=14, Encoding=65001, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Filtered Rows1" = Table.SelectRows(#"Promoted Headers", each ([GEO_LEVEL] = "2")),
    #"Merged Queries" = Table.NestedJoin(#"Filtered Rows1", {"Member ID: Profile of Forward Sortation Areas (2247)"}, Current_Members, {"MemberId"}, "Current_Members", JoinKind.LeftOuter),
    #"Removed Columns" = Table.RemoveColumns(#"Merged Queries",{"Current_Members", "ALT_GEO_CODE", "DATA_QUALITY_FLAG", "Notes: Profile of Forward Sortation Areas (2247)"}),
    #"Replaced Value" = Table.ReplaceValue(#"Removed Columns","2","A0011",Replacer.ReplaceText,{"GEO_LEVEL"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"CENSUS_YEAR", "REF_DATE"}, {"GEO_CODE (POR)", "GEO_CODE"}, {"GEO_NAME", "GEO"}, {"GNR", "Short form: Non-response"}, {"GNR_LF", "Long form: Non-response"}, {"DIM: Profile of Forward Sortation Areas (2247)", "Member"}, {"Member ID: Profile of Forward Sortation Areas (2247)", "MemberId"}, {"Dim: Sex (3): Member ID: [1]: Total - Sex", "Total"}, {"Dim: Sex (3): Member ID: [2]: Male", "Male"}, {"Dim: Sex (3): Member ID: [3]: Female", "Female"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "REF_DATE", "REF_DATE - Copy"),
    #"Merged Columns" = Table.CombineColumns(#"Duplicated Column",{"REF_DATE - Copy", "GEO_LEVEL", "GEO_CODE"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"DGUID")
in
    #"Merged Columns";

shared Current_Members = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Parameters\Profile_Indicators.csv"),[Delimiter=",", Columns=13, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"ThemeID", Int64.Type}, {"ThemeName_EN", type text}, {"Sex_Dimension", Int64.Type}, {"MemberId", Int64.Type}, {"MemberParentID", Int64.Type}, {"MemberName_EN", type text}, {"UOM", type text}, {"UOM_ID", Int64.Type}, {"SCALAR_FACTOR", type text}, {"SCALAR_ID", Int64.Type}, {"DECIMALS", Int64.Type}}),
    #"Removed Other Columns" = Table.SelectColumns(#"Changed Type",{"ThemeID", "MemberId"}),
    #"Filtered Rows" = Table.SelectRows(#"Removed Other Columns", each [ThemeID] = Theme_to_Process),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows",{"ThemeID"}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Removed Columns",{{"MemberId", type text}})
in
    #"Changed Type1";

shared Theme_to_Process = 3 meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true];