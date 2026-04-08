[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "4c779583-e813-4409-944e-402b0d824e9f"]}[Data]{[lakehouseId = "65b4c4ab-0ac9-449c-a49c-f1a20da9726d"]}[Data];
[BindToDefaultDestination = true]
shared Business = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/MicrosoftLearning/mslearn-fabric/refs/heads/main/Allfiles/Labs/22a/Store.csv"), [Delimiter = "|", Columns = 7, Encoding = 1200, QuoteStyle = QuoteStyle.None]),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"Column1", Int64.Type}, {"Column2", type text}, {"Column3", Int64.Type}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}}),
  #"Rename columns" = Table.RenameColumns(#"Changed column type", {{"Column1", "BusinessEntityID"}, {"Column2", "Name"}, {"Column3", "SalesPersonID"}, {"Column4", "Demographics"}, {"Column5", "rowguid"}, {"Column6", "ModifiedDate"}}),
  #"Transform columns" = Table.TransformColumns(#"Rename columns", {{"Name", each Text.Start(_, Text.Length(_) - 1), type text}, {"Demographics", each Text.Start(_, Text.Length(_) - 1), type text}, {"rowguid", each Text.Start(_, Text.Length(_) - 1), type text}}),
  #"Remove columns" = Table.RemoveColumns(#"Transform columns", {"rowguid", "Column7"}),
  #"Transform columns 1" = Table.TransformColumns(#"Remove columns", {{"Demographics", each Text.Replace(_, Character.FromNumber(65279), ""), type text}}),
  #"Expanded table column" = Table.ExpandRecordColumn(
    Table.TransformColumns(
        #"Transform columns 1",
        {
            {
                "Demographics",
                each Xml.Tables(_){0},
                type record
            }
        }
    ),
    "Demographics",
    {"AnnualSales", "AnnualRevenue", "BankName", "BusinessType", "YearOpened", "Specialty", "SquareFeet", "Brands", "Internet", "NumberEmployees"}
),
  #"Transform columns 2" = Table.TransformColumns(#"Expanded table column", {{"ModifiedDate", each Text.Start(_, Text.Length(_) - 1), type text}}),
  #"Change type" = Table.TransformColumnTypes(#"Transform columns 2", {{"ModifiedDate", type datetime}}),
  #"Change type 1" = Table.TransformColumnTypes(#"Change type", {{"AnnualSales", Int64.Type}, {"AnnualRevenue", Int64.Type}, {"SquareFeet", Int64.Type}, {"NumberEmployees", Int64.Type}}),
  #"Add column" = Table.AddColumn(#"Change type 1", "StoreSize", each if [SquareFeet] >= 0 and [SquareFeet] <= 10000 then "Small"
         else if [SquareFeet] >= 10001 and [SquareFeet] <= 40000 then "Medium"
         else if [SquareFeet] >= 40001 and [SquareFeet] <= 80000 then "Large"
         else null, type text),
  #"Change type 2" = Table.TransformColumnTypes(#"Add column", {{"BankName", type text}, {"BusinessType", type text}, {"YearOpened", type text}, {"Specialty", type text}, {"Brands", type text}, {"Internet", type text}}),
  #"Duplicated column" = Table.DuplicateColumn(#"Change type 2", "StoreSize", "StoreSize - Copy"),
  #"Duplicated column 1" = Table.DuplicateColumn(#"Duplicated column", "ModifiedDate", "ModifiedDate - Copy"),
  #"Renamed columns" = Table.RenameColumns(#"Duplicated column 1", {{"StoreSize - Copy", "Category"}, {"ModifiedDate - Copy", "NewDate"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"NewDate", type text}}),
  #"Change type 3" = Table.TransformColumnTypes(#"Changed column type 1", {{"BusinessEntityID", Int64.Type}, {"Name", type text}, {"SalesPersonID", Int64.Type}, {"AnnualSales", type number}, {"AnnualRevenue", type number}, {"BankName", type text}, {"BusinessType", type text}, {"YearOpened", Int64.Type}, {"Specialty", type text}, {"SquareFeet", Int64.Type}, {"Brands", type text}, {"Internet", type text}, {"NumberEmployees", Int64.Type}, {"ModifiedDate", type datetime}, {"StoreSize", type text}, {"Category", type text}, {"NewDate", type datetime}})
in
  #"Change type 3";
