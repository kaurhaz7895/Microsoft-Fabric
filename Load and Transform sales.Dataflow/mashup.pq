[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "fda9f6fb-9246-4947-8599-d182d5a7c5b3"]}[Data]{[lakehouseId = "6f535fd5-bef8-4561-8749-0f305de9572d"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sales = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "fda9f6fb-9246-4947-8599-d182d5a7c5b3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "6f535fd5-bef8-4561-8749-0f305de9572d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "bronze"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "sales.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"OrderDate", type date}, {"CustomerName", type text}, {"EmailAddress", type text}, {"Item", type text}, {"Quantity", Int64.Type}, {"UnitPrice", type number}, {"TaxAmount", type number}})
in
  #"Changed column type";
shared #"sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "fda9f6fb-9246-4947-8599-d182d5a7c5b3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "6f535fd5-bef8-4561-8749-0f305de9572d"]}[Data],
  TableNavigation = Navigation_2{[Id = "sales csv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
