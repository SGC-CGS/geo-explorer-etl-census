section Section1;

shared IndicatorTheme_EN = let
    Source = Cube_EN,
    #"Cube Title" = Source{0}[Cube Title]
in
    #"Cube Title";

shared ReleaseDate = let
    Source = Cube_EN,
    #"Start Reference Period" = Source{0}[Start Reference Period]
in
    #"Start Reference Period";

shared IndicatorTheme_FR = let
    Source = Cube_EN,
    #"Cube Title" = Source{0}[Cube Title]
in
    #"Cube Title";

shared StatisticsProgramId = 98 meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true];

[ Description = "The type of break algorithm applied to the data: #(lf)1=Equal Interval#(lf)2=Natural#(lf)3=Quantile" ]
shared BreakAlgorithm = 2 meta [IsParameterQuery=true, List={1, 2, 3}, DefaultValue=2, Type="Binary", IsParameterQueryRequired=true];

[ Description = "The number of breaks that the DV tool will display on the map." ]
shared Breaks = 6 meta [IsParameterQuery=true, List={1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, DefaultValue=5, Type="Binary", IsParameterQueryRequired=true];

shared ProductId = "984010038" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

[ Description = "The start Color of the breaks (Generally a lighter color)" ]
shared ColorFrom = "225,190,231,255" meta [IsParameterQuery=true, List={"225,220,255,255", "165,230,255,255", "245,120,200,255"}, DefaultValue="225,220,255,255", Type="Text", IsParameterQueryRequired=true];

shared ColorTo = "123,31,162,255" meta [IsParameterQuery=true, List={"235,0,235,250", "0,170,240,250", "210,15,140,250"}, DefaultValue="235,0,235,250", Type="Text", IsParameterQueryRequired=true];

shared Dimension_Start_Ticket = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="SELECT MAX (DimensionID + 1)#(lf)FROM gis.Dimensions", CreateNavigationProperties=false]),
    #"Transposed Table" = Table.Transpose(Source),
    #"Replaced Value" = Table.ReplaceValue(#"Transposed Table",null,1,Replacer.ReplaceValue,{"Column1"}),
    #"Select the Value" = #"Replaced Value"{0}[Column1]
in
    #"Select the Value";

shared DimensionValues_Start_Ticket = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="SELECT MAX (DimensionValueId + 1)#(lf)FROM gis.DimensionValues", CreateNavigationProperties=false]),
    #"Transposed Table" = Table.Transpose(Source),
    #"Replaced Value" = Table.ReplaceValue(#"Transposed Table",null,1,Replacer.ReplaceValue,{"Column1"}),
    #"Select the Value" = #"Replaced Value"{0}[Column1]
in
    #"Select the Value";

shared A_IndicatorTheme = let
    Source = Cube_EN,
    #"Removed Columns" = Table.RemoveColumns(Source,{"CANSIM Id", "Cube Notes"}),
    IndicatorThemeDescription_EN = Table.AddColumn(#"Removed Columns", "IndicatorThemeDescription_EN", each [Cube Title]&" | "&[Archive Status]),
    IndicatorThemeDescription_FR = Table.AddColumn(IndicatorThemeDescription_EN, "IndicatorThemeDescription_FR", each [Cube Title]&" | "&[Archive Status]),
    #"Renamed Columns" = Table.RenameColumns(IndicatorThemeDescription_FR,{{"Product Id", "IndicatorThemeId"}, {"Cube Title", "IndicatorTheme_EN"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "IndicatorTheme_EN", "IndicatorTheme_FR"),
    #"Inserted First Characters" = Table.AddColumn(#"Duplicated Column", "ParentThemeId", each Text.Start(Text.From([IndicatorThemeId], "en-CA"), 5), type text),
    #"Added Custom" = Table.AddColumn(#"Inserted First Characters", "StatisticsProgramId", each StatisticsProgramId),
    #"Removed Columns1" = Table.RemoveColumns(#"Added Custom",{"URL", "Archive Status", "Frequency", "Start Reference Period", "End Reference Period", "Total number of dimensions"}),
    #"Reordered Columns" = Table.ReorderColumns(#"Removed Columns1",{"IndicatorThemeId", "IndicatorTheme_EN", "IndicatorTheme_FR", "StatisticsProgramId", "IndicatorThemeDescription_EN", "IndicatorThemeDescription_FR", "ParentThemeId"}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Reordered Columns",{{"IndicatorThemeId", Int64.Type}, {"IndicatorTheme_EN", type text}, {"IndicatorTheme_FR", type text}, {"StatisticsProgramId", Int64.Type}, {"IndicatorThemeDescription_EN", type text}, {"IndicatorThemeDescription_FR", type text}, {"ParentThemeId", Int64.Type}}),
    #"Changed Type with Locale" = Table.TransformColumnTypes(#"Changed Type2", {{"IndicatorTheme_FR", type text}}, "fr-CA"),
    #"Changed Type with Locale1" = Table.TransformColumnTypes(#"Changed Type with Locale", {{"IndicatorThemeDescription_FR", type text}}, "fr-CA")
in
    #"Changed Type with Locale1";

shared B_Dimensions = let
    // The Dimensions query uses the query titled: getCubeMetaData as its source. The getCubeMetaData is utilized as the input source for multiple queries in this ETL application.  
    Source = Dimensions,
    // The dimensions table in the database requires an indicator theme to link the dimensions to. This query uses the ProductId that was saved as a variable from the getCubeMetaData query and adds it to the Dimensions table.
    #"Added IndicatorThemeId" = Table.AddColumn(Source, "IndicatorThemeId", each ProductId),
    #"Added Index" = Table.AddIndexColumn(#"Added IndicatorThemeId", "Index", 0, 1),
    #"Added Custom" = Table.AddColumn(#"Added Index", "DimensionId", each Dimension_Start_Ticket + [Index]),
    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"Index"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"Dimension ID", "DisplayOrder"}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Renamed Columns",{{"DimensionId", Int64.Type}, {"IndicatorThemeId", Int64.Type}, {"DisplayOrder", Int64.Type}, {"Dimension_EN", type text}, {"Dimension_FR", type text}}),
    #"Reordered Columns" = Table.ReorderColumns(#"Changed Type",{"DimensionId", "IndicatorThemeId", "Dimension_EN", "Dimension_FR", "DisplayOrder", "DimensionType"}),
    #"Changed Type with Locale" = Table.TransformColumnTypes(#"Reordered Columns", {{"Dimension_FR", type text}}, "fr-CA")
in
    #"Changed Type with Locale";

shared C_DimensionValues = let
    Source = DimensionValues_EN,
    #"Added Custom" = Table.AddColumn(Source, "IndicatorThemeId", each (ProductId)),
    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"Terminated", "Classification Code", "Member Notes", "Member Definitions"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Removed Columns",{{"IndicatorThemeId", Int64.Type}}),
    #"Merged Queries" = Table.NestedJoin(#"Changed Type",{"Dimension ID", "IndicatorThemeId"},B_Dimensions,{"DisplayOrder", "IndicatorThemeId"},"Dimensions",JoinKind.LeftOuter),
    #"Expanded Dimensions" = Table.ExpandTableColumn(#"Merged Queries", "Dimensions", {"DimensionId"}, {"DimensionId"}),
    #"Removed Columns1" = Table.RemoveColumns(#"Expanded Dimensions",{"Dimension ID"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Columns1", "Index", 0, 1),
    #"Added DimensionValueId" = Table.AddColumn(#"Added Index", "DimensionValueId", each (DimensionValues_Start_Ticket) +[Index]),
    #"Added Conditional Column" = Table.AddColumn(#"Added DimensionValueId", "Custom", each if [Member ID] < 10 then 0 else null),
    #"Changed Type4" = Table.TransformColumnTypes(#"Added Conditional Column",{{"Custom", type text}}),
    #"Changed Type1" = Table.TransformColumnTypes(#"Changed Type4",{{"Member ID", type text}}),
    #"Replaced Value" = Table.ReplaceValue(#"Changed Type1",null,"",Replacer.ReplaceValue,{"Custom"}),
    #"Added Display_EN" = Table.AddColumn(#"Replaced Value", "Display_EN", each if [memberNameEn] = "2016" then [memberNameEn] else [Custom]&[Member ID]&". "&[memberNameEn]),
    #"Added Display_FR" = Table.AddColumn(#"Added Display_EN", "Display_FR", each if [memberNameEn] = "2016" then [memberNameFr] else [Custom]&[Member ID]&". "&[memberNameFr]),
    #"Removed Columns2" = Table.RemoveColumns(#"Added Display_FR",{"Index", "memberNameEn", "memberNameFr", "IndicatorThemeId", "Custom"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns2",{{"Member ID", "ValueDisplayOrder"}, {"Parent Member ID", "ValueDisplayParent"}}),
    #"Changed Type3" = Table.TransformColumnTypes(#"Renamed Columns",{{"ValueDisplayParent", Int64.Type}, {"ValueDisplayOrder", Int64.Type}, {"DimensionValueId", Int64.Type}}),
    #"Changed Type2" = Table.TransformColumnTypes(#"Changed Type3",{{"DimensionValueId", Int64.Type}, {"DimensionId", Int64.Type}, {"Display_EN", type text}, {"Display_FR", type text}}),
    #"Changed Type with Locale" = Table.TransformColumnTypes(#"Changed Type2", {{"Display_FR", type text}}, "fr-CA")
in
    #"Changed Type with Locale";

shared Product_EN = let
    Source = Csv.Document(File.Contents("\\stchsfsb\descpau$\CENSUS_ETL\Products\" & ProductId & "\Product_EN" & ".csv"),[Delimiter=",", Columns=16, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Removed Columns" = Table.RemoveColumns(#"Promoted Headers",{"TERMINATED", "SYMBOL", "SCALAR_FACTOR", "SCALAR_ID"}),
    #"Extracted Text After Delimiter" = Table.TransformColumns(#"Removed Columns", {{"COORDINATE", each Text.AfterDelimiter(_, "."), type text}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Extracted Text After Delimiter", "REF_DATE", "Year"),
    #"Changed Type" = Table.TransformColumnTypes(#"Duplicated Column",{{"Year", type date}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "IndicatorCode", each ProductId &"."&[COORDINATE]&"."& Date.ToText([Year])),
    #"Filtered Rows" = Table.SelectRows(#"Added Custom", each ([REF_DATE] = "2016"))
in
    #"Filtered Rows";

shared Product_FR = let
    Source = Product_EN
in
    Source;

shared NullReasons = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="SELECT NullReasonId, Symbol#(lf)FROM gis.IndicatorNullReason", CreateNavigationProperties=false])
in
    Source;

shared GeographySource = let
    Source = Table.SelectColumns(Product_EN,{"DGUID", "IndicatorCode"}),
    #"Merged Queries" = Table.NestedJoin(Source,{"IndicatorCode"},D_Indicator,{"IndicatorCode"},"Indicator",JoinKind.LeftOuter),
    #"Expanded Indicator" = Table.ExpandTableColumn(#"Merged Queries", "Indicator", {"IndicatorId"}, {"IndicatorId"})
in
    #"Expanded Indicator";

shared IndicatorTitle_EN = let
    Source = Dimensions,
    #"Removed Other Columns1" = Table.SelectColumns(Source,{"Dimension_EN"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns1",{{"Dimension_EN", "Title_Columns_EN"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","Date","REF_DATE",Replacer.ReplaceText,{"Title_Columns_EN"}),
    Title_Columns_EN = #"Replaced Value"[Title_Columns_EN]
in
    Title_Columns_EN;

shared IndicatorTitle_FR = let
    Source = Dimensions,
    #"Removed Other Columns" = Table.SelectColumns(Source,{"Dimension_FR"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns",{{"Dimension_FR", "Title_Columns_FR"}}),
    #"Replaced Value" = Table.ReplaceValue(#"Renamed Columns","Date","REF_DATE",Replacer.ReplaceText,{"Title_Columns_FR"}),
    Title_Columns_EN = #"Replaced Value"[Title_Columns_FR]
in
    Title_Columns_EN;

shared IndicatorNames_EN = let
    Source = Table.SelectColumns(Product_EN, List.Combine({{"IndicatorCode", "UOM", "UOM_ID", "VECTOR"},IndicatorTitle_EN})),
    #"Removed Duplicates" = Table.Distinct(Source, {"IndicatorCode"}),
    #"Merged Columns" = Table.CombineColumns(#"Removed Duplicates", List.Combine({IndicatorTitle_EN}), Combiner.CombineTextByDelimiter(" | ", QuoteStyle.None),"IndicatorName_EN"),
    #"Added Custom" = Table.AddColumn(#"Merged Columns", "IndicatorDisplay_EN", each "<ul><li>"&[IndicatorName_EN]&"</li><br/><b>Statistics Canada.</b>"&(Citation_FR)&"</ul>"),
    #"Replaced Value" = Table.ReplaceValue(#"Added Custom"," | ","<li>",Replacer.ReplaceText,{"IndicatorDisplay_EN"})
in
    #"Replaced Value";

shared IndicatorNames_FR = let
    Source = Table.SelectColumns(Product_FR, List.Combine({{"IndicatorCode", "UOM", "UOM_ID", "VECTOR"},IndicatorTitle_FR})),
    #"Removed Duplicates" = Table.Distinct(Source, {"IndicatorCode"}),
    #"Merged Columns" = Table.CombineColumns(#"Removed Duplicates", List.Combine({IndicatorTitle_FR}), Combiner.CombineTextByDelimiter(" | ", QuoteStyle.None),"IndicatorName_FR"),
    #"Added Custom" = Table.AddColumn(#"Merged Columns", "IndicatorDisplay_FR", each "<ul><li>"&[IndicatorName_FR]&"</li><br/><b>Statistique Canada.</b>"&(Citation_FR)&"</ul>"),
    #"Replaced Value" = Table.ReplaceValue(#"Added Custom"," | ","<li>",Replacer.ReplaceText,{"IndicatorDisplay_FR"}),
    #"Renamed Columns" = Table.RenameColumns(#"Replaced Value",{{"UOM", "UOM_FR"}})
in
    #"Renamed Columns";

shared D_Indicator = let
    Source = Table.SelectColumns(IndicatorNames_EN, {"IndicatorCode", "UOM", "UOM_ID", "IndicatorName_EN", "IndicatorDisplay_EN", "VECTOR"}),
    #"Merged Queries" = Table.NestedJoin(Source,{"IndicatorCode"},IndicatorNames_FR,{"IndicatorCode"},"IndicatorNames_FR",JoinKind.Inner),
    #"Expanded IndicatorNames_FR" = Table.ExpandTableColumn(#"Merged Queries", "IndicatorNames_FR", {"UOM_FR", "IndicatorName_FR", "IndicatorDisplay_FR"}, {"UOM_FR", "IndicatorName_FR", "IndicatorDisplay_FR"}),
    #"Added Index" = Table.AddIndexColumn(#"Expanded IndicatorNames_FR", "IndicatorId", Indicator_Start_Ticket, 1),
    #"Added Custom1" = Table.AddColumn(#"Added Index", "IndicatorThemeId", each ProductId),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "ReleaseIndicatorDate", each ReleaseDate),
    #"Inserted Last Characters" = Table.AddColumn(#"Added Custom2", "ReferencePeriod", each Text.End([IndicatorCode], 10), type text),
    #"Changed Type" = Table.TransformColumnTypes(#"Inserted Last Characters",{{"IndicatorId", Int64.Type}, {"IndicatorName_EN", type text}, {"IndicatorName_FR", type text}, {"IndicatorThemeId", Int64.Type}, {"ReleaseIndicatorDate", type datetime}, {"ReferencePeriod", type date}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"UOM", "UOM_EN"}}),
    #"Extracted Text After Delimiter" = Table.TransformColumns(#"Renamed Columns", {{"VECTOR", each Text.AfterDelimiter(_, "v98"), type text}}),
    #"Changed Type with Locale" = Table.TransformColumnTypes(#"Extracted Text After Delimiter", {{"UOM_FR", type text}}, "fr-CA"),
    #"Changed Type with Locale1" = Table.TransformColumnTypes(#"Changed Type with Locale", {{"IndicatorName_FR", type text}}, "fr-CA"),
    #"Changed Type with Locale2" = Table.TransformColumnTypes(#"Changed Type with Locale1", {{"IndicatorDisplay_FR", type text}}, "fr-CA"),
    #"Duplicated Column" = Table.DuplicateColumn(#"Changed Type with Locale2", "IndicatorName_EN", "IndicatorNameLong_EN"),
    #"Duplicated Column1" = Table.DuplicateColumn(#"Duplicated Column", "IndicatorName_FR", "IndicatorNameLong_FR"),
    #"Changed Type1" = Table.TransformColumnTypes(#"Duplicated Column1",{{"VECTOR", Int64.Type}}),
    #"Replaced Value" = Table.ReplaceValue(#"Changed Type1","|","_",Replacer.ReplaceText,{"IndicatorNameLong_EN"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","|","_",Replacer.ReplaceText,{"IndicatorNameLong_FR"})
in
    #"Replaced Value1";

shared Indicator_Start_Ticket = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="SELECT MAX (IndicatorID + 1)#(lf)FROM gis.Indicator", CreateNavigationProperties=false]),
    #"Transposed Table" = Table.Transpose(Source),
    #"Replaced Value" = Table.ReplaceValue(#"Transposed Table",null,1,Replacer.ReplaceValue,{"Column1"}),
    #"Select the Value" = #"Replaced Value"{0}[Column1]
in
    #"Select the Value";

shared IndicatorValues_Start_Ticket = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="SELECT MAX (IndicatorValueID + 1)#(lf)FROM gis.IndicatorValues", CreateNavigationProperties=false]),
    #"Transposed Table" = Table.Transpose(Source),
    #"Replaced Value" = Table.ReplaceValue(#"Transposed Table",null,1,Replacer.ReplaceValue,{"Column1"}),
    #"Select the Value" = #"Replaced Value"{0}[Column1]
in
    #"Select the Value";

shared H_IndicatorMetaData = let
    Source = Table.SelectColumns(D_Indicator, {"IndicatorCode", "IndicatorId", "UOM_EN", "UOM_ID"}),
    #"Renamed Columns" = Table.RenameColumns(Source,{{"UOM_ID", "DataFormatId"}, {"UOM_EN", "FieldAlias_EN"}}),
    #"Merged Queries" = Table.NestedJoin(#"Renamed Columns", {"IndicatorCode"}, IndicatorNames_FR, {"IndicatorCode"}, "IndicatorNames_FR", JoinKind.LeftOuter),
    #"Expanded IndicatorNames_FR" = Table.ExpandTableColumn(#"Merged Queries", "IndicatorNames_FR", {"UOM_FR"}, {"FieldAlias_FR"}),
    #"Added Custom" = Table.AddColumn(#"Expanded IndicatorNames_FR", "MetaDataId", each [IndicatorId]),
    #"Added DefaultBreaks" = Table.AddColumn(#"Added Custom", "DefaultBreaksAlgorithmId", each (BreakAlgorithm)),
    #"Added Custom1" = Table.AddColumn(#"Added DefaultBreaks", "DefaultBreaks", each (Breaks)),
    #"Added Custom2" = Table.AddColumn(#"Added Custom1", "PrimaryChartTypeId", each 1),
    #"Added English Format" = Table.AddColumn(#"Added Custom2", "Format_EN", each if [DataFormatId] = "223" then "Format(iv.value, 'N0', 'en-US')" else if [DataFormatId] = "81" then "Format(iv.value, 'C0', 'en-US')" else if [DataFormatId] = "239" then "Format((iv.value/100), 'P1', 'en-US')" else "Format(iv.value, 'N', 'en-US')"),
    #"Added French Format" = Table.AddColumn(#"Added English Format", "Format_FR", each if [DataFormatId] = "223" then "Format(iv.value, 'N0', 'fr-CA')" else if [DataFormatId] = "81" then "Format(iv.value, 'C0', 'fr-CA')" else if [DataFormatId] = "239" then "Format((iv.value/100), 'P1', 'fr-CA')" else "Format(iv.value, 'N', 'fr-CA')"),
    #"Added Conditional Column" = Table.AddColumn(#"Added French Format", "ColorTo", each (ColorTo)),
    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "ColorFrom", each (ColorFrom)),
    #"Merged Queries2" = Table.NestedJoin(#"Added Conditional Column1",{"IndicatorCode"},#"KEY (2)",{"IndicatorCode"},"KEY (2)",JoinKind.LeftOuter),
    #"Expanded KEY (2)" = Table.ExpandTableColumn(#"Merged Queries2", "KEY (2)", {"DimensionUniqueKey"}, {"DimensionUniqueKey"}),
    #"Added Custom4" = Table.AddColumn(#"Expanded KEY (2)", "DefaultRelatedChartId", each [IndicatorId]),
    #"Added Custom3" = Table.AddColumn(#"Added Custom4", "PrimaryQuery", each "SELECT iv.value AS Value, CASE WHEN iv.value IS NULL THEN nr.symbol ELSE "&[Format_EN]&" END AS FormattedValue_EN,  CASE WHEN iv.value IS NULL THEN nr.symbol ELSE "&[Format_FR]&" END AS FormattedValue_FR, grfi.GeographyReferenceId, g.DisplayNameShort_EN, g.DisplayNameShort_FR, g.DisplayNameLong_EN, g.DisplayNameLong_FR, g.ProvTerrName_EN, g.ProvTerrName_FR, g.Shape, i.IndicatorName_EN, i.IndicatorName_FR, i.IndicatorId, i.IndicatorDisplay_EN, i.IndicatorDisplay_FR, i.UOM_EN, i.UOM_FR, g.GeographicLevelId, gl.LevelName_EN, gl.LevelName_FR, gl.LevelDescription_EN, gl.LevelDescription_FR, g.EntityName_EN, g.EntityName_FR, nr.Symbol, nr.Description_EN as NullDescription_EN, nr.Description_FR as NullDescription_FR FROM gis.geographyreference AS g INNER JOIN gis.geographyreferenceforindicator AS grfi ON g.geographyreferenceid = grfi.geographyreferenceid  INNER JOIN (select * from gis.indicator where indicatorId = "&Number.ToText([IndicatorId])&") AS i ON grfi.indicatorid = i.indicatorid  INNER JOIN gis.geographiclevel AS gl ON g.geographiclevelid = gl.geographiclevelid  INNER JOIN gis.geographiclevelforindicator AS glfi  ON i.indicatorid = glfi.indicatorid  AND gl.geographiclevelid = glfi.geographiclevelid  INNER JOIN gis.indicatorvalues AS iv  ON iv.indicatorvalueid = grfi.indicatorvalueid  INNER JOIN gis.indicatortheme AS it ON i.indicatorthemeid = it.indicatorthemeid  LEFT OUTER JOIN gis.indicatornullreason AS nr  ON iv.nullreasonid = nr.nullreasonid"),
    #"Changed Type with Locale" = Table.TransformColumnTypes(#"Added Custom3", {{"FieldAlias_FR", type text}}, "fr-CA")
in
    #"Changed Type with Locale";

shared KEY_Dimensions = let
    Source = C_DimensionValues,
    #"Added Custom" = Table.AddColumn(Source, "IndicatorThemeId", each (ProductId)),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Custom",{{"IndicatorThemeId", Int64.Type}}),
    #"Merged Queries1" = Table.NestedJoin(#"Changed Type", {"DimensionId"}, B_Dimensions, {"DimensionId"}, "Dimensions", JoinKind.LeftOuter),
    #"Expanded Dimensions2" = Table.ExpandTableColumn(#"Merged Queries1", "Dimensions", {"Dimension_EN", "Dimension_FR", "DisplayOrder", "DimensionType"}, {"Dimension_EN", "Dimension_FR", "DisplayOrder", "DimensionType"}),
    #"Reordered Columns" = Table.ReorderColumns(#"Expanded Dimensions2",{"DimensionType", "ValueDisplayOrder", "ValueDisplayParent", "DisplayOrder", "IndicatorThemeId", "Dimension_EN", "Dimension_FR", "DimensionId", "DimensionValueId", "Display_EN", "Display_FR"}),
    #"Renamed Columns" = Table.RenameColumns(#"Reordered Columns",{{"Dimension_EN", "dimensionNameEn"}, {"Dimension_FR", "dimensionNameFr"}}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Renamed Columns", "Display_EN", "memberNameEn1"),
    #"Duplicated Column1" = Table.DuplicateColumn(#"Duplicated Column", "Display_FR", "memberNameFr1"),
    #"Extracted Text Before Delimiter" = Table.TransformColumns(#"Duplicated Column1", {{"memberNameEn1", each Text.AfterDelimiter(_, ". "), type text}}),
    #"Extracted Text After Delimiter" = Table.TransformColumns(#"Extracted Text Before Delimiter", {{"memberNameFr1", each Text.AfterDelimiter(_, ". "), type text}}),
    #"Added Conditional Column" = Table.AddColumn(#"Extracted Text After Delimiter", "memberNameEn", each if [dimensionNameEn] = "Date" then [Display_EN] else [memberNameEn1]),
    #"Added Conditional Column1" = Table.AddColumn(#"Added Conditional Column", "memberNameFr", each if [dimensionNameFr] = "Date" then [Display_FR] else [memberNameFr1])
in
    #"Added Conditional Column1";

shared #"KEY (2)" = let
    Source = Product_EN,
    #"Removed Other Columns" = Table.SelectColumns(Source, List.Combine({{"IndicatorCode","MemberId"},IndicatorTitle_EN})),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Other Columns",{{"REF_DATE", "Date"}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Renamed Columns",{"Member"}),
    #"Removed Duplicates" = Table.Distinct(#"Removed Columns1", {"IndicatorCode"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Duplicates",{{"MemberId", "Member"}}),
    #"Unpivoted Other Columns" = Table.UnpivotOtherColumns(#"Renamed Columns1", {"IndicatorCode"}, "Attribute", "Value"),
    #"Merged Queries" = Table.NestedJoin(#"Unpivoted Other Columns", {"Value", "Attribute"}, KEY_Dimensions, {"UniqueMemberId", "dimensionNameEn"}, "KEY_Dimensions", JoinKind.LeftOuter),
    #"Expanded KEY_Dimensions" = Table.ExpandTableColumn(#"Merged Queries", "KEY_Dimensions", {"DimensionValueId"}, {"DimensionValueId"}),
    #"Added Conditional Column" = Table.AddColumn(#"Expanded KEY_Dimensions", "Custom", each if [DimensionValueId] = null then [Value] else [DimensionValueId]),
    #"Changed Type1" = Table.TransformColumnTypes(#"Added Conditional Column",{{"Custom", Int64.Type}}),
    #"Removed Columns2" = Table.RemoveColumns(#"Changed Type1",{"DimensionValueId"}),
    #"Renamed Columns2" = Table.RenameColumns(#"Removed Columns2",{{"Custom", "DimensionValueId"}}),
    #"Merged Queries1" = Table.NestedJoin(#"Renamed Columns2",{"Attribute"},B_Dimensions,{"Dimension_EN"},"Dimensions",JoinKind.LeftOuter),
    #"Expanded Dimensions" = Table.ExpandTableColumn(#"Merged Queries1", "Dimensions", {"DisplayOrder"}, {"DisplayOrder"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Expanded Dimensions",{{"DisplayOrder", type text}}),
    #"Added Custom" = Table.AddColumn(#"Changed Type", "Custom", each [DisplayOrder]&". "&[Attribute]),
    #"Removed Columns" = Table.RemoveColumns(#"Added Custom",{"DisplayOrder", "Attribute", "Value"}),
    #"Pivoted Column" = Table.Pivot(#"Removed Columns", List.Distinct(#"Removed Columns"[Custom]), "Custom", "DimensionValueId"),
    #"Reordered Columns" = Table.ReorderColumns(#"Pivoted Column",{"IndicatorCode", "1. Date", "2. Member"}),
    #"Added Custom1" = Table.AddColumn(#"Reordered Columns", "DimensionUniqueKey", each Text.Combine(List.Transform(List.Skip(Record.FieldValues(_)),Text.From),"-")),
    #"Removed Other Columns1" = Table.SelectColumns(#"Added Custom1",{"IndicatorCode", "DimensionUniqueKey"})
in
    #"Removed Other Columns1";

shared Related_Manual = let
    Source = #"KEY (2)",
    #"Split Column by Delimiter" = Table.SplitColumn(Source, "DimensionUniqueKey", Splitter.SplitTextByDelimiter("-", QuoteStyle.Csv), {"DimensionUniqueKey.1", "DimensionUniqueKey.2", "DimensionUniqueKey.3", "DimensionUniqueKey.4"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Split Column by Delimiter",{{"DimensionUniqueKey.1", Int64.Type}, {"DimensionUniqueKey.2", Int64.Type}, {"DimensionUniqueKey.3", Int64.Type}, {"DimensionUniqueKey.4", Int64.Type}}),
    #"Merged Queries" = Table.NestedJoin(#"Changed Type",{"IndicatorCode"},D_Indicator,{"IndicatorCode"},"Indicator",JoinKind.LeftOuter),
    #"Expanded Indicator" = Table.ExpandTableColumn(#"Merged Queries", "Indicator", {"IndicatorId"}, {"Indicator.IndicatorId"})
in
    #"Expanded Indicator";

shared G_GeographyReferenceForIndicator = let
    Source = GeographySource,
    #"Added Custom" = Table.AddColumn(Source, "IndicatorValueCode", each [DGUID]&"."&[IndicatorCode]),
    #"Merged Queries" = Table.NestedJoin(#"Added Custom",{"DGUID"},GeographyReference,{"GeographyReferenceId"},"GeographyReference",JoinKind.LeftOuter),
    #"Expanded GeographyReference" = Table.ExpandTableColumn(#"Merged Queries", "GeographyReference", {"GeographyReferenceId"}, {"GeographyReferenceId"}),
    #"Filtered Rows1" = Table.SelectRows(#"Expanded GeographyReference", each [GeographyReferenceId] <> null and [GeographyReferenceId] <> ""),
    #"Removed Columns1" = Table.RemoveColumns(#"Filtered Rows1",{"GeographyReferenceId"}),
    #"Merged Queries1" = Table.NestedJoin(#"Removed Columns1",{"IndicatorValueCode"},F_IndicatorValues,{"IndicatorValueCode"},"IndicatorValues",JoinKind.LeftOuter),
    #"Expanded IndicatorValues" = Table.ExpandTableColumn(#"Merged Queries1", "IndicatorValues", {"IndicatorValueId"}, {"IndicatorValueId"}),
    #"Renamed Columns" = Table.RenameColumns(#"Expanded IndicatorValues",{{"DGUID", "GeographyReferenceId"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns", each [GeographyReferenceId] <> null and [GeographyReferenceId] <> ""),
    #"Inserted Text After Delimiter" = Table.AddColumn(#"Filtered Rows", "Text After Delimiter", each Text.AfterDelimiter([IndicatorCode], ".", 4), type text),
    #"Removed Columns" = Table.RemoveColumns(#"Inserted Text After Delimiter",{"IndicatorCode", "IndicatorValueCode"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns",{{"Text After Delimiter", "ReferencePeriod"}})
in
    #"Renamed Columns1";

shared F_IndicatorValues = let
    Source = Table.SelectColumns(Product_EN,{"DGUID", "IndicatorCode", "STATUS", "Value"}),
    #"Merged Queries1" = Table.NestedJoin(Source,{"DGUID"},GeographyReference,{"GeographyReferenceId"},"GeographyReference",JoinKind.LeftOuter),
    #"Expanded GeographyReference" = Table.ExpandTableColumn(#"Merged Queries1", "GeographyReference", {"GeographyReferenceId"}, {"GeographyReferenceId"}),
    #"Filtered Rows" = Table.SelectRows(#"Expanded GeographyReference", each [GeographyReferenceId] <> null and [GeographyReferenceId] <> ""),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows",{"GeographyReferenceId"}),
    #"Merged Columns" = Table.CombineColumns(#"Removed Columns",{"DGUID", "IndicatorCode"},Combiner.CombineTextByDelimiter(".", QuoteStyle.None),"IndicatorValueCode"),
    #"Merged Queries" = Table.NestedJoin(#"Merged Columns", {"STATUS"}, NullReasons, {"Symbol"}, "NullReasons", JoinKind.LeftOuter),
    #"Expanded NullReasons" = Table.ExpandTableColumn(#"Merged Queries", "NullReasons", {"NullReasonId"}, {"NullReasonId"}),
    #"Removed Other Columns" = Table.SelectColumns(#"Expanded NullReasons",{"IndicatorValueCode", "NullReasonId", "Value"}),
    #"Added Index" = Table.AddIndexColumn(#"Removed Other Columns", "IndicatorValueId", IndicatorValues_Start_Ticket, 1),
    #"Changed Type" = Table.TransformColumnTypes(#"Added Index",{{"IndicatorValueId", Int64.Type}, {"Value", type number}, {"NullReasonId", Int64.Type}})
in
    #"Changed Type";

shared E_GeographyLevelForIndicator = let
    Source = GeographySource,
    #"Extracted Text Range" = Table.TransformColumns(Source, {{"DGUID", each Text.Middle(_, 4, 5), type text}}),
    #"Replaced Value" = Table.ReplaceValue(#"Extracted Text Range","S0504","S0503",Replacer.ReplaceText,{"DGUID"}),
    #"Replaced Value1" = Table.ReplaceValue(#"Replaced Value","S0505","S0503",Replacer.ReplaceText,{"DGUID"}),
    #"Replaced Value2" = Table.ReplaceValue(#"Replaced Value1","S0506","S0503",Replacer.ReplaceText,{"DGUID"}),
    #"Removed Duplicates" = Table.Distinct(#"Replaced Value2"),
    #"Filtered Rows" = Table.SelectRows(#"Removed Duplicates", each [DGUID] <> null and [DGUID] <> ""),
    #"Removed Columns" = Table.RemoveColumns(#"Filtered Rows",{"IndicatorCode"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"DGUID", "GeographicLevelId"}}),
    #"Filtered Rows1" = Table.SelectRows(#"Renamed Columns", each ([IndicatorId] <> null))
in
    #"Filtered Rows1";

shared Database = "STC_DV" meta [IsParameterQuery=true, List={"STC_DV", "ISTD_CHSP"}, DefaultValue="STC_DV", Type="Text", IsParameterQueryRequired=true];

shared GeographyReference = let
    Source = Sql.Database("SQLB-DB-D05", Database, [Query="Select GeographyReferenceId#(lf)FROM gis.GEOGRAPHYREFERENCE", CreateNavigationProperties=false])
in
    Source;

shared FrequencyCode = let
    Source = Cube_EN,
    Frequency = Source{0}[Frequency]
in
    Frequency;

shared Cube_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Products\" & ProductId & "\Cube_EN.csv"),[Delimiter=",", Columns=10, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Cube Title", type text}, {"Product Id", Int64.Type}, {"CANSIM Id", type text}, {"URL", type text}, {"Cube Notes", type text}, {"Archive Status", type text}, {"Frequency", type text}, {"Start Reference Period", type date}, {"End Reference Period", type date}, {"Total number of dimensions", Int64.Type}})
in
    #"Changed Type";

shared Dimensions = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Products\" & ProductId & "\Dimensions_EN.csv"),[Delimiter=",", Columns=4, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Dimension ID", Int64.Type}, {"Dimension_EN", type text}, {"Dimension_FR", type text}})
in
    #"Changed Type";

shared DimensionValues_EN = let
    Source = Csv.Document(File.Contents("H:\CENSUS_ETL\Products\" & ProductId & "\DimensionValues_EN.csv"),[Delimiter=",", Columns=10, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promoted Headers" = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    #"Changed Type" = Table.TransformColumnTypes(#"Promoted Headers",{{"Dimension ID", Int64.Type}, {"memberNameEn", type text}, {"memberNameFr", type text}, {"Classification Code", type text}, {"Member ID", Int64.Type}, {"Parent Member ID", type text}, {"Terminated", type text}, {"Member Notes", type text}, {"Member Definitions", type text}})
in
    #"Changed Type";

shared Citation_EN = "2016 Census Profile" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];

shared Citation_FR = "2016 Profil du recensement" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true];