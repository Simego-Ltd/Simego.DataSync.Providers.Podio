using System;
using System.Collections.Generic;

namespace Simego.DataSync.Providers.Podio
{
    internal enum PodioDataSchemaItemType
    {
        String,
        Number,
        DateTime,
        Duration,
        Category,
        Money,
        App,
        Contact,
        Image,
        Link,
        Question,
        Email,
        Phone,
        Location
    }
           
    internal class PodioDataSchemaItem
    {
        public string Name { get; set; }
        public string SubName { get; set; }
        public string DisplayName { get; set; }

        public PodioDataSchemaItemType PodioDataType { get; set; }
        public Type DataType { get; set; }

        public bool Unique { get; set; }

        public bool AllowNull { get; set; }

        public int Length { get; set; }

        public bool IsSubValue { get { return !IsRoot && !string.IsNullOrEmpty(SubName); } }
        public bool IsRootSubValue { get { return IsRoot && !string.IsNullOrEmpty(SubName); } }
        public bool IsRoot { get; set; }
        public bool IsMultiValue { get; set; }
        public bool IsMultiIndexValue { get; set; }

        public int Index { get; set; }

        public bool ReadOnly { get; set; }

        public bool TimeDisabled { get; set; }
        public string TimeUtcField { get; set; }

        public Dictionary<string, int> LookupValues { get; set; }

        public List<PodioDataSchemaDependency> Dependencies { get; set; }

        public PodioDataSchemaItem()
        {
            Length = -1;
            LookupValues = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            Dependencies = new List<PodioDataSchemaDependency>();
        }
    }

    internal class PodioDataSchemaDependency
    {
        public string Name { get; set; }
        public bool Required { get; set; }
    }

    internal class PodioDataSchema
    {
        public int SpaceID { get; set; }
        public string Token { get; set; }
        public Dictionary<string, PodioDataSchemaItem> Columns { get; set; }

        public static PodioDataSchema FromJson(string schema)
        {
            return FromJson(Json.Decode(schema));
        }

        public static PodioDataSchema FromJson(dynamic schema)
        {
            PodioDataSchema result = new PodioDataSchema()
            {
                Token = schema["token"],
                SpaceID = schema["space_id"],
                Columns = new Dictionary<string, PodioDataSchemaItem>()
            };

            result.Columns.Add("external_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "external_id",
                                   DisplayName = "external_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   Unique = false,
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("item_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "item_id",
                                   DisplayName = "item_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(long),
                                   Unique = true,
                                   AllowNull = false,
                                   IsRoot = true
                               });

            result.Columns.Add("app_item_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "app_item_id",
                                   DisplayName = "app_item_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(int),
                                   Unique = true,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("app_item_id_formatted",
                               new PodioDataSchemaItem
                               {
                                   Name = "app_item_id_formatted",
                                   DisplayName = "app_item_id_formatted",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   Unique = true,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("created_on",
                               new PodioDataSchemaItem
                               {
                                   Name = "created_on",
                                   DisplayName = "created_on",
                                   PodioDataType = PodioDataSchemaItemType.DateTime,
                                   DataType = typeof(DateTime),
                                   Unique = false,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("last_event_on",
                               new PodioDataSchemaItem
                               {
                                   Name = "last_event_on",
                                   DisplayName = "last_event_on",
                                   PodioDataType = PodioDataSchemaItemType.DateTime,
                                   DataType = typeof(DateTime),
                                   Unique = false,
                                   AllowNull = true,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("created_by|user_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "created_by",
                                   DisplayName = "created_by|user_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   SubName = "user_id",
                                   DataType = typeof(int),
                                   Unique = false,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("created_by|name",
                               new PodioDataSchemaItem
                               {
                                   Name = "created_by",
                                   DisplayName = "created_by|name",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   SubName = "name",
                                   DataType = typeof(String),
                                   Unique = false,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });


            foreach (var field in schema["fields"])
            {
                if ((string)field["status"] != "active")
                    continue;

                switch ((string)field["type"])
                {
                    case "date":
                        {
                            bool timeEnabled = true;
                            if (field.ContainsKey("config") && field["config"].ContainsKey("settings") && field["config"]["settings"].ContainsKey("time"))
                            {
                                timeEnabled = field["config"]["settings"]["time"].Equals("enabled", StringComparison.OrdinalIgnoreCase);
                            }

                            result.Columns.Add(string.Format("{0}|startdate", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|startdate", field["label"]),
                                                   SubName = timeEnabled ? "start_utc" : "start_date",
                                                   TimeUtcField = "start_time_utc",
                                                   PodioDataType = PodioDataSchemaItemType.DateTime,
                                                   Dependencies = new List<PodioDataSchemaDependency>(new[] { new PodioDataSchemaDependency { Name = string.Format("{0}|enddate", field["external_id"]) } }),
                                                   DataType = typeof(DateTime),
                                                   TimeDisabled = !timeEnabled,
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|enddate", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|enddate", field["label"]),
                                                   SubName = timeEnabled ? "end_utc" : "end_date",
                                                   TimeUtcField = "end_time_utc",
                                                   PodioDataType = PodioDataSchemaItemType.DateTime,
                                                   Dependencies = new List<PodioDataSchemaDependency>(new[] { new PodioDataSchemaDependency { Name = string.Format("{0}|startdate", field["external_id"]), Required = true } }),
                                                   DataType = typeof(DateTime),
                                                   TimeDisabled = !timeEnabled,
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "money":
                        {

                            result.Columns.Add(string.Format("{0}|amount", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|amount", field["label"]),
                                                   SubName = "value",
                                                   PodioDataType = PodioDataSchemaItemType.Money,
                                                   Dependencies = new List<PodioDataSchemaDependency>(new[] { new PodioDataSchemaDependency { Name = string.Format("{0}|currency", field["external_id"]) } }),
                                                   DataType = typeof(decimal),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|currency", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|currency", field["label"]),
                                                   SubName = "currency",
                                                   PodioDataType = PodioDataSchemaItemType.Money,
                                                   Dependencies = new List<PodioDataSchemaDependency>(new[] { new PodioDataSchemaDependency { Name = string.Format("{0}|amount", field["external_id"]) } }),
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "number":
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   PodioDataType = PodioDataSchemaItemType.Number,
                                                   DataType = typeof(decimal),
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "duration":
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   PodioDataType = PodioDataSchemaItemType.Number,
                                                   DataType = typeof(int),
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "text":
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   PodioDataType = PodioDataSchemaItemType.String,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "app":
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   SubName = "item_id",
                                                   PodioDataType = PodioDataSchemaItemType.App,
                                                   IsMultiValue = true,
                                                   DataType = typeof(long[]),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|title", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|title", field["label"]),
                                                   SubName = "title",
                                                   PodioDataType = PodioDataSchemaItemType.App,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string[]),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            break;
                        }
                    case "contact":
                        {

                            result.Columns.Add(string.Format("{0}|user_id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|user_id", field["label"]),
                                                   SubName = "user_id",
                                                   PodioDataType = PodioDataSchemaItemType.Contact,
                                                   IsMultiValue = true,
                                                   DataType = typeof(long[]),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|profile_id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|profile_id", field["label"]),
                                                   SubName = "profile_id",
                                                   PodioDataType = PodioDataSchemaItemType.Contact,
                                                   IsMultiValue = true,
                                                   DataType = typeof(long[]),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|connection_id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|connection_id", field["label"]),
                                                   SubName = "connection_id",
                                                   PodioDataType = PodioDataSchemaItemType.Contact,
                                                   IsMultiValue = true,
                                                   DataType = typeof(long[]),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|external_id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|external_id", field["label"]),
                                                   SubName = "external_id",
                                                   PodioDataType = PodioDataSchemaItemType.Contact,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string[]),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|name", field["external_id"]),
                                              new PodioDataSchemaItem
                                              {
                                                  Name = field["external_id"],
                                                  DisplayName = string.Format("{0}|name", field["label"]),
                                                  SubName = "name",
                                                  PodioDataType = PodioDataSchemaItemType.Contact,
                                                  IsMultiValue = true,
                                                  DataType = typeof(string[]),
                                                  AllowNull = true,
                                                  ReadOnly = true
                                              });

                            break;
                        }
                    case "category":
                        {
                            bool multiValue = PodioDataSchemaTypeConverter.ConvertToInvariant<bool>(field["config"]["settings"]["multiple"]);
                            Dictionary<string, int> lookupValues = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            foreach (var val in field["config"]["settings"]["options"])
                                if (val.ContainsKey("status") && val["status"] == "active")
                                    lookupValues[val["text"]] = val["id"];

                            result.Columns.Add(string.Format("{0}|id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|id", field["label"]),
                                                   SubName = "id",
                                                   IsMultiValue = true,
                                                   PodioDataType = PodioDataSchemaItemType.Category,
                                                   DataType = multiValue ? typeof(int[]) : typeof(int),
                                                   LookupValues = lookupValues,
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|text", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|text", field["label"]),
                                                   SubName = "text",
                                                   IsMultiValue = true,
                                                   PodioDataType = PodioDataSchemaItemType.Category,
                                                   DataType = multiValue ? typeof(string[]) : typeof(string),
                                                   LookupValues = lookupValues,
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "question":
                        {
                            bool multiValue = PodioDataSchemaTypeConverter.ConvertToInvariant<bool>(field["config"]["settings"]["multiple"]);
                            Dictionary<string, int> lookupValues = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                            foreach (var val in field["config"]["settings"]["options"])
                                if (val.ContainsKey("status") && val["status"] == "active")
                                    lookupValues[val["text"]] = val["id"];

                            result.Columns.Add(string.Format("{0}|id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|id", field["label"]),
                                                   SubName = "id",
                                                   IsMultiValue = true,
                                                   PodioDataType = PodioDataSchemaItemType.Question,
                                                   DataType = multiValue ? typeof(int[]) : typeof(int),
                                                   LookupValues = lookupValues,
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|text", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|text", field["label"]),
                                                   SubName = "text",
                                                   IsMultiValue = true,
                                                   PodioDataType = PodioDataSchemaItemType.Question,
                                                   DataType = multiValue ? typeof(string[]) : typeof(string),
                                                   LookupValues = lookupValues,
                                                   AllowNull = true
                                               });

                            break;
                        }
                    case "progress":
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   PodioDataType = PodioDataSchemaItemType.Number,
                                                   DataType = typeof(int),
                                                   AllowNull = true
                                               });
                            break;
                        }
                    case "image":
                        {

                            result.Columns.Add(string.Format("{0}|id", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|id", field["label"]),
                                                   SubName = "file_id",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(int),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|name", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|name", field["label"]),
                                                   SubName = "name",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|description", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|description", field["label"]),
                                                   SubName = "description",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|mimetype", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|mimetype", field["label"]),
                                                   SubName = "mimetype",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            result.Columns.Add(string.Format("{0}|size", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|size", field["label"]),
                                                   SubName = "size",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(int),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            result.Columns.Add(string.Format("{0}|perma_link", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|perma_link", field["label"]),
                                                   SubName = "perma_link",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            result.Columns.Add(string.Format("{0}|link", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|link", field["label"]),
                                                   SubName = "link",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            result.Columns.Add(string.Format("{0}|thumbnail_link", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|thumbnail_link", field["label"]),
                                                   SubName = "thumbnail_link",
                                                   PodioDataType = PodioDataSchemaItemType.Image,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });




                            break;
                        }

                    case "embed":
                        {
                            result.Columns.Add(string.Format("{0}|id", field["external_id"]),
                                              new PodioDataSchemaItem
                                              {
                                                  Name = field["external_id"],
                                                  DisplayName = string.Format("{0}|id", field["label"]),
                                                  SubName = "embed_id",
                                                  PodioDataType = PodioDataSchemaItemType.Link,
                                                  IsMultiValue = true,
                                                  DataType = typeof(int),
                                                  AllowNull = true
                                              });

                            result.Columns.Add(string.Format("{0}|original_url", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|original_id", field["label"]),
                                                   SubName = "original_url",
                                                   PodioDataType = PodioDataSchemaItemType.Link,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = false
                                               });


                            result.Columns.Add(string.Format("{0}|resolved_url", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|resolved_url", field["label"]),
                                                   SubName = "resolved_url",
                                                   PodioDataType = PodioDataSchemaItemType.Link,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true,
                                                   ReadOnly = true
                                               });

                            result.Columns.Add(string.Format("{0}|title", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|title", field["label"]),
                                                   SubName = "title",
                                                   PodioDataType = PodioDataSchemaItemType.Link,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|description", field["external_id"]),
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = string.Format("{0}|description", field["label"]),
                                                   SubName = "description",
                                                   PodioDataType = PodioDataSchemaItemType.Link,
                                                   IsMultiValue = true,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });

                            result.Columns.Add(string.Format("{0}|type", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|type", field["label"]),
                                                      SubName = "email",
                                                      PodioDataType = PodioDataSchemaItemType.Link,
                                                      IsMultiValue = true,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = true
                                                  });

                            break;

                        }
                    case "email":
                        {

                            result.Columns.Add(string.Format("{0}|work", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|work", field["label"]),
                                                      SubName = "work",
                                                      PodioDataType = PodioDataSchemaItemType.Email,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|home", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|home", field["label"]),
                                                      SubName = "home",
                                                      PodioDataType = PodioDataSchemaItemType.Email,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|other", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|other", field["label"]),
                                                      SubName = "other",
                                                      PodioDataType = PodioDataSchemaItemType.Email,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });
                            break;
                        }
                    case "phone":
                        {

                            result.Columns.Add(string.Format("{0}|mobile", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|mobile", field["label"]),
                                                      SubName = "mobile",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|home", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|home", field["label"]),
                                                      SubName = "home",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|work", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|work", field["label"]),
                                                      SubName = "work",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|main", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|main", field["label"]),
                                                      SubName = "main",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|work_fax", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|work_fax", field["label"]),
                                                      SubName = "work_fax",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|private_fax", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|private_fax", field["label"]),
                                                      SubName = "private_fax",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|other", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}|other", field["label"]),
                                                      SubName = "other",
                                                      PodioDataType = PodioDataSchemaItemType.Phone,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            break;
                        }
                    case "location":
                        {

                            result.Columns.Add(string.Format("{0}", field["external_id"]),
                                                  new PodioDataSchemaItem
                                                  {
                                                      Name = field["external_id"],
                                                      DisplayName = string.Format("{0}", field["label"]),
                                                      SubName = "value",
                                                      PodioDataType = PodioDataSchemaItemType.Location,
                                                      DataType = typeof(string),
                                                      AllowNull = true,
                                                      ReadOnly = false
                                                  });

                            result.Columns.Add(string.Format("{0}|formatted", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|formatted", field["label"]),
                                                     SubName = "formatted",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|street_number", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|street_number", field["label"]),
                                                     SubName = "street_number",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|street_name", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|street_name", field["label"]),
                                                     SubName = "street_name",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|city", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|city", field["label"]),
                                                     SubName = "city",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|state", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|state", field["label"]),
                                                     SubName = "state",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|postal_code", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|postal_code", field["label"]),
                                                     SubName = "postal_code",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|country", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|country", field["label"]),
                                                     SubName = "country",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(string),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });



                            result.Columns.Add(string.Format("{0}|lat", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|lat", field["label"]),
                                                     SubName = "lat",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(double),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|lng", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|lng", field["label"]),
                                                     SubName = "lng",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(double),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            result.Columns.Add(string.Format("{0}|map_in_sync", field["external_id"]),
                                                 new PodioDataSchemaItem
                                                 {
                                                     Name = field["external_id"],
                                                     DisplayName = string.Format("{0}|map_in_sync", field["label"]),
                                                     SubName = "map_in_sync",
                                                     PodioDataType = PodioDataSchemaItemType.Location,
                                                     DataType = typeof(bool),
                                                     AllowNull = true,
                                                     ReadOnly = false
                                                 });

                            break;
                        }


                    default:
                        {
                            result.Columns.Add(field["external_id"],
                                               new PodioDataSchemaItem
                                               {
                                                   Name = field["external_id"],
                                                   DisplayName = field["label"],
                                                   PodioDataType = PodioDataSchemaItemType.String,
                                                   DataType = typeof(string),
                                                   AllowNull = true
                                               });
                            break;
                        }
                }
            }

            return result;
        }

        public DataSchema ToDataSchema()
        {
            DataSchema schema = new DataSchema();

            foreach (var column in Columns.Keys)
            {
                schema.Map.Add(new DataSchemaItem(column, Columns[column].DisplayName, Columns[column].DataType, Columns[column].Unique, false, Columns[column].AllowNull, Columns[column].Length) { ReadOnly = Columns[column].ReadOnly });
            }

            return schema;
        }

        public static PodioDataSchema Contacts()
        {
            PodioDataSchema result = new PodioDataSchema() { Columns = new Dictionary<string, PodioDataSchemaItem>() };

            result.Columns.Add("external_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "external_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   Unique = false,
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("profile_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "profile_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(long),
                                   Unique = true,
                                   AllowNull = false,
                                   IsRoot = true,
                                   ReadOnly = true
                               });

            result.Columns.Add("name",
                               new PodioDataSchemaItem
                               {
                                   Name = "name",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("title",
                               new PodioDataSchemaItem
                               {
                                   Name = "title",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("organization",
                               new PodioDataSchemaItem
                               {
                                   Name = "organization",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("emailaddress1",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = true
                               });

            result.Columns.Add("emailaddress2",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = true
                               });

            result.Columns.Add("emailaddress3",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = true
                               });


            result.Columns.Add("phone1",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = true
                               });

            result.Columns.Add("phone2",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = true
                               });

            result.Columns.Add("phone3",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = true
                               });


            result.Columns.Add("phone4",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 3,
                                   IsRoot = true
                               });


            result.Columns.Add("address1",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = true
                               });

            result.Columns.Add("address2",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = true
                               });

            result.Columns.Add("address3",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = true
                               });

            result.Columns.Add("city",
                               new PodioDataSchemaItem
                               {
                                   Name = "city",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("state",
                               new PodioDataSchemaItem
                               {
                                   Name = "state",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });


            result.Columns.Add("zip",
                               new PodioDataSchemaItem
                               {
                                   Name = "zip",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });


            result.Columns.Add("country",
                               new PodioDataSchemaItem
                               {
                                   Name = "country",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("skype",
                               new PodioDataSchemaItem
                               {
                                   Name = "skype",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("about",
                               new PodioDataSchemaItem
                               {
                                   Name = "about",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = true
                               });



            return result;
        }

        public static PodioDataSchema Users()
        {
            PodioDataSchema result = new PodioDataSchema() { Columns = new Dictionary<string, PodioDataSchemaItem>() };

            result.Columns.Add("external_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "external_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   Unique = false,
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("profile_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "profile_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(int),
                                   Unique = false,
                                   AllowNull = false,
                                   IsRoot = false,
                                   SubName = "profile",
                                   ReadOnly = true
                               });

            result.Columns.Add("user_id",
                               new PodioDataSchemaItem
                               {
                                   Name = "user_id",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(int),
                                   Unique = true,
                                   AllowNull = false,
                                   IsRoot = false,
                                   SubName = "profile",
                                   ReadOnly = true
                               });

            result.Columns.Add("type",
                               new PodioDataSchemaItem
                               {
                                   Name = "type",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("employee",
                               new PodioDataSchemaItem
                               {
                                   Name = "employee",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(bool),
                                   AllowNull = true,
                                   IsRoot = true
                               });

            result.Columns.Add("role",
                                new PodioDataSchemaItem
                                {
                                    Name = "role",
                                    PodioDataType = PodioDataSchemaItemType.String,
                                    DataType = typeof(string),
                                    AllowNull = true,
                                    IsRoot = true
                                });

            result.Columns.Add("name",
                               new PodioDataSchemaItem
                               {
                                   Name = "name",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("title",
                               new PodioDataSchemaItem
                               {
                                   Name = "title",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("organization",
                               new PodioDataSchemaItem
                               {
                                   Name = "organization",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("emailaddress1",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("emailaddress2",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("emailaddress3",
                               new PodioDataSchemaItem
                               {
                                   Name = "mail",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = false,
                                   SubName = "profile"
                               });


            result.Columns.Add("phone1",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("phone2",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("phone3",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = false,
                                   SubName = "profile"
                               });


            result.Columns.Add("phone4",
                               new PodioDataSchemaItem
                               {
                                   Name = "phone",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 3,
                                   IsRoot = false,
                                   SubName = "profile"
                               });


            result.Columns.Add("address1",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 0,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("address2",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 1,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("address3",
                               new PodioDataSchemaItem
                               {
                                   Name = "address",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsMultiIndexValue = true,
                                   Index = 2,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("city",
                               new PodioDataSchemaItem
                               {
                                   Name = "city",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("state",
                               new PodioDataSchemaItem
                               {
                                   Name = "state",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });


            result.Columns.Add("zip",
                               new PodioDataSchemaItem
                               {
                                   Name = "zip",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });


            result.Columns.Add("country",
                               new PodioDataSchemaItem
                               {
                                   Name = "country",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("location",
                               new PodioDataSchemaItem
                               {
                                   Name = "location",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("avatar",
                               new PodioDataSchemaItem
                               {
                                   Name = "avatar",
                                   PodioDataType = PodioDataSchemaItemType.Number,
                                   DataType = typeof(int),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("skype",
                               new PodioDataSchemaItem
                               {
                                   Name = "skype",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("twitter",
                               new PodioDataSchemaItem
                               {
                                   Name = "twitter",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("linkedin",
                               new PodioDataSchemaItem
                               {
                                   Name = "linkedin",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("about",
                               new PodioDataSchemaItem
                               {
                                   Name = "about",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("birthdate",
                               new PodioDataSchemaItem
                               {
                                   Name = "birthdate",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("url",
                               new PodioDataSchemaItem
                               {
                                   Name = "url",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("skills",
                               new PodioDataSchemaItem
                               {
                                   Name = "skill",
                                   PodioDataType = PodioDataSchemaItemType.String,
                                   DataType = typeof(string[]),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });

            result.Columns.Add("last_seen_on",
                               new PodioDataSchemaItem
                               {
                                   Name = "last_seen_on",
                                   PodioDataType = PodioDataSchemaItemType.DateTime,
                                   DataType = typeof(DateTime),
                                   AllowNull = true,
                                   IsRoot = false,
                                   SubName = "profile"
                               });
            return result;
        }

        public static IList<string> GetRelatedSchemaItems(PodioDataSchemaItem item, IDictionary<string, PodioDataSchemaItem> items)
        {
            // { emailaddress1, emailaddress2, emailaddress3 }
            // { emailaddress1, null, emailaddress3 }


            List<string> relatedColumns = new List<string>();

            foreach (var key in items.Keys)
            {
                if (items[key].Name == item.Name)
                {
                    relatedColumns.Add(key);
                }
            }

            return relatedColumns;

        }
    }

}