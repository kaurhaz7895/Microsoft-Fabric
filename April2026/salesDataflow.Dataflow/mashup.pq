[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "File", TransformationQueryName = "sales csv_DataDestinationTransform"]]]}]
shared #"sales csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4c779583-e813-4409-944e-402b0d824e9f"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "65b4c4ab-0ac9-449c-a49c-f1a20da9726d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "bronze"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "sales.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Use first row as headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Use first row as headers", {{"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"OrderDate", type date}, {"CustomerName", type text}, {"EmailAddress", type text}, {"Item", type text}, {"Quantity", Int64.Type}, {"UnitPrice", type number}, {"TaxAmount", type number}}),
  #"Add column" = Table.AddColumn(#"Changed column type", "TotalAmount", each [Quantity] * [UnitPrice] + [TaxAmount]),
  #"Change type" = Table.TransformColumnTypes(#"Add column", {{"TotalAmount", type number}})
in
  #"Change type";
shared #"sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4c779583-e813-4409-944e-402b0d824e9f"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "65b4c4ab-0ac9-449c-a49c-f1a20da9726d"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Files", ItemKind = "Folder"]}[Data],
  Navigation_4 = Navigation_3{[Name = "bronze"]}[Content],
  FileNavigation = Navigation_4{[Name = "sales2.csv"]}?[Content]?
in
  FileNavigation;
shared #"sales csv_DataDestinationTransform" = (binaryStream, columnNameAndTypePairs) => let
  Pattern = Csv.Document(binaryStream, [Columns = List.Transform(columnNameAndTypePairs, each _{0}), CsvStyle = CsvStyle.QuoteAfterDelimiter, IncludeByteOrderMark = true, ExtraValues = ExtraValues.Ignore, Delimiter = ",", Encoding = 65001]),
  PromoteHeaders = Table.PromoteHeaders(Pattern),
  TransformColumnTypes = Table.TransformColumnTypes(PromoteHeaders, columnNameAndTypePairs, [MissingField = MissingField.Ignore])
in
  TransformColumnTypes;
