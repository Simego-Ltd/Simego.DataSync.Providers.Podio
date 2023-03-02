using Simego.DataSync.Helpers;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.Podio.TypeConverters;
using Simego.DataSync.Providers.Podio.TypeEditors;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing.Design;

namespace Simego.DataSync.Providers.Podio
{
    [ProviderInfo(Name = "Podio to DB App Schema", Description = "Returns Information about Space Apps as DB Schema Information", Group = "Podio")]
    [ProviderIgnore]
    public class PodioDbSchemaDataSourceReader : DataReadOnlyReaderProviderBase, IDataSourceRegistry
    {
        enum DbSchemaColumnDataType
        {
            Integer,
            BigInteger,
            Decimal,
            Boolean,
            DateTime,
            VarString,
            Text,
            UniqueIdentifier,
            Blob
        }

        enum DbSchemaColumnDefault
        {
            None,
            NewUniqueIdentifier,
            CurrentDateTime,
            Zero,
            One,
            Two,
            Three
        }

        private IDataSourceRegistryProvider _registryProvider;
        private ConnectionInterface _connectionIf;
        private string _space;

        [Description("Podio Service Credentials")]
        [Category("Service")]
        [Editor(typeof(OAuthCredentialsWebTypeEditor), typeof(UITypeEditor))]
        public string Credentials { get; set; }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal string AccessToken { get; set; }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal string RefreshToken { get; set; }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal DateTime TokenExpires { get; set; } = new DateTime(1970, 1, 1);
        
        [Category("Podio Api")]
        [ReadOnly(true)]
        public int ApiRateLimit { get; set; }

        [Category("Podio Api")]
        [ReadOnly(true)]
        public int ApiRateRemaining { get; set; }

        [Category("Connection")]
        [TypeConverter(typeof(PodioSpaceTypeConverter))]
        [Description("Space in Podio to connect")]
        public string Space
        {
            get { return _space; }
            set
            {
                if (_space != value)
                {
                    if (ValidateToken())
                    {
                        if (GetOrgSpaces().TryGetValue(value, out int val))
                        {
                            SpaceID = val;
                        }
                    }
                }
                _space = value;
            }
        }

        [Category("Connection")]
        [Description("Name of Schema to return")]
        public string SchemaName { get; set; } = "dbo";

        [Category("Connection")]
        [Description("ID of Space in Podio")]
        [ReadOnly(true)]
        public int SpaceID { get; set; }
       
        [Category("OAuth")]
        [Description("OAuth Client ID")]
        [Browsable(false)]
        [ProviderCacheSetting(Name = "PodioDataSourceReader.ClientID")]
        public string ClientID { get; set; }

        [Category("OAuth")]
        [Description("OAuth Client Secret")]
        [Browsable(false)]
        [ProviderCacheSetting(Name = "PodioDataSourceReader.ClientSecret")]
        public string ClientSecret { get; set; }
        
        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            ValidateToken();
            
            var mapping = new DataSchemaMapping(SchemaMap, Side);
            var columns = SchemaMap.GetIncludedColumns();
            var hashHelper = new HashHelper(HashHelper.HashType.MD5);

            HttpWebRequestHelper helper = new HttpWebRequestHelper
            {
                UserAgent = PodioHelper.USER_AGENT,
                AuthorizationHeader = $"OAuth2 {AccessToken}"
            };

            foreach (var app in PodioHelper.GetOrgApps(AccessToken, SpaceID))
            {
                var app_id = app.Value;
                var app_name = app.Key;
                var defaultVarStringLength = 1000;

                var result = helper.GetRequestAsJson($"https://api.podio.com/app/{app_id}");

                UpdateRateLimits(helper.ResponseHeaders);

                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "item_id", DbSchemaColumnDataType.BigInteger, -1, 0, 0, true, true, true);
                AddIndexRow(dt, mapping, columns, hashHelper, SchemaName, "TABLE_CONSTRAINT", app_name, "item_id", true, true, new[] { "item_id" });
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "external_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "app_item_id", DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, true);
                AddIndexRow(dt, mapping, columns, hashHelper, SchemaName, "TABLE_CONSTRAINT", app_name, "app_item_id", false, true, new[] { "app_item_id" });
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "created_on", DbSchemaColumnDataType.DateTime, -1, 0, 0, false, false, false);
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "last_event_on", DbSchemaColumnDataType.DateTime, -1, 0, 0, false, false, false);
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "created_by|user_id", DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, false);
                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, "created_by|name", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                
                foreach (var row in result["fields"])
                {                    
                    var podioColumnType = row["type"]?.ToObject<string>() ?? "text";
                    
                    switch (podioColumnType)
                    {
                        case "app":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|title", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "contact":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|user_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|profile_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|connection_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|external_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|name", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "phone":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|mobile", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|home", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|work", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|main", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|work_fax", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|private_fax", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|other", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "email":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|work", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|home", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|other", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "number":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, row["external_id"].ToObject<string>(), DbSchemaColumnDataType.Decimal, -1, 16, 4, false, false, false);
                                break;
                            }
                        case "money":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|amount", DbSchemaColumnDataType.Decimal, -1, 16, 4, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|currency", DbSchemaColumnDataType.VarString, 10, 0, 0, false, false, false);
                                break;

                            }
                        case "embed":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|id", DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|original_id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|resolved_url", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|title", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|description", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|type", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "image":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|id", DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|name", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|description", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|mimetype", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|size", DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|perma_link", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|thumbnail_link", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "location":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, row["external_id"].ToObject<string>(), DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|formatted", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|street_number", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|street_name", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|city", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|state", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|postal_code", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|country", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|lat", DbSchemaColumnDataType.Decimal, -1, 8, 6, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|lng", DbSchemaColumnDataType.Decimal, -1, 8, 6, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|map_in_sync", DbSchemaColumnDataType.Boolean, -1, 0, 0, false, false, false);
                                break;

                            }
                        case "question":
                        case "category":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|id", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|text", DbSchemaColumnDataType.VarString, defaultVarStringLength, 0, 0, false, false, false);
                                break;

                            }
                        case "date":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|startdate", DbSchemaColumnDataType.DateTime, -1, 0, 0, false, false, false);
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, $"{row["external_id"].ToObject<string>()}|enddate", DbSchemaColumnDataType.DateTime, -1, 0, 0, false, false, false);
                                break;

                            }
                        case "progress":
                        case "duration":
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, row["external_id"].ToObject<string>(), DbSchemaColumnDataType.Integer, -1, 0, 0, false, false, false);
                                break;

                            }
                        default:
                            {
                                AddRow(dt, mapping, columns, SchemaName, "TABLE_COLUMN", app_name, row["external_id"].ToObject<string>(), DbSchemaColumnDataType.Text, -1, 0, 0, false, false, false);
                                break;
                            }
                    }
                    
                }
            }
            return dt;
        }

        private int AddRow(DataTableStore dt, DataSchemaMapping mapping, IList<DataSchemaItem> columns, string schema, string objectType, string tableName, string name, DbSchemaColumnDataType dataType, int length, int precision, int scale, bool notNull, bool primaryKey, bool isUnique)
        {
            return dt.Rows.Add(mapping, columns,
                        (item, columnName) =>
                        {
                            switch (columnName)
                            {
                                case "Schema":
                                    {
                                        return schema;
                                    }
                                case "ObjectType":
                                    {
                                        return objectType;
                                    }
                                case "TableName":
                                    {
                                        return tableName;
                                    }
                                case "Name":
                                    {
                                        return name;
                                    }
                                case "DataType":
                                    {
                                        return dataType.ToString();
                                    }
                                case "Length":
                                    {
                                        return length;
                                    }
                                case "Precision":
                                    {
                                        return precision;
                                    }
                                case "Scale":
                                    {
                                        return scale;
                                    }
                                case "NotNull":
                                    {
                                        return notNull;
                                    }
                                case "ColumnDefault":
                                    {
                                        return DbSchemaColumnDefault.None.ToString();
                                    }
                                case "IsIdentity":
                                    {
                                        return false;
                                    }
                                case "IsPrimaryKey":
                                    {
                                        return primaryKey;
                                    }
                                case "IsClustered":
                                    {
                                        return primaryKey;
                                    }
                                case "IsUnique":
                                    {
                                        return isUnique;
                                    }
                                case "Include":
                                    {
                                        return null;
                                    }
                                case "Columns":
                                    {
                                        return null;
                                    }
                                default:
                                    {
                                        return null;
                                    }
                            }

                        });
        }

        private int AddIndexRow(DataTableStore dt, DataSchemaMapping mapping, IList<DataSchemaItem> columns, HashHelper hashHelper, string schema, string objectType, string tableName, string name, bool primaryKey, bool isUnique, string [] indexColumns)
        {
            return dt.Rows.Add(mapping, columns,
                        (item, columnName) =>
                        {
                            switch (columnName)
                            {
                                case "Schema":
                                    {
                                        return schema;
                                    }
                                case "ObjectType":
                                    {
                                        return objectType;
                                    }
                                case "TableName":
                                    {
                                        return tableName;
                                    }
                                case "Name":
                                    {
                                        var hash = hashHelper.GetHashAsString(DataSchemaTypeConverter.ConvertTo<string>(indexColumns)).Substring(0, 6);
                                        return $"{(primaryKey ? "PK" : "IX")}_{schema}_{tableName}_{hash}";                                        
                                    }
                                case "Length":
                                    {
                                        return 0;
                                    }
                                case "Precision":
                                    {
                                        return 0;
                                    }
                                case "Scale":
                                    {
                                        return 0;
                                    }
                                case "NotNull":
                                    {
                                        return false;
                                    }
                                case "IsIdentity":
                                    {
                                        return false;
                                    }
                                case "IsPrimaryKey":
                                    {
                                        return primaryKey;
                                    }
                                case "IsClustered":
                                    {
                                        return primaryKey;
                                    }
                                case "IsUnique":
                                    {
                                        return isUnique;
                                    }
                                case "Columns":
                                    {
                                        return indexColumns;
                                    }
                                default:
                                    {
                                        return null;
                                    }
                            }

                        });
        }

        public override DataSchema GetDefaultDataSchema()
        {
            DataSchema schema = new DataSchema();

            schema.Map.Add(new DataSchemaItem("Schema", typeof(string), true, false, false, -1));
            schema.Map.Add(new DataSchemaItem("ObjectType", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("TableName", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Name", typeof(string), true, false, true, -1));
            schema.Map.Add(new DataSchemaItem("DataType", typeof(string), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Length", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Precision", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Scale", typeof(int), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("NotNull", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("ColumnDefault", typeof(string), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsIdentity", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsPrimaryKey", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsClustered", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("IsUnique", typeof(bool), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Include", typeof(string[]), false, false, true, -1));
            schema.Map.Add(new DataSchemaItem("Columns", typeof(string[]), false, false, true, -1));

            return schema;
        }

        private void UpdateRateLimits(IDictionary<string, string> responseHeaders)
        {
            int val;
            if (int.TryParse(responseHeaders["X-Rate-Limit-Limit"], out val))
            {
                ApiRateLimit = val;
            }
            if (int.TryParse(responseHeaders["X-Rate-Limit-Remaining"], out val))
            {
                ApiRateRemaining = val;
            }
        }

        private bool ValidateToken()
        {
            if (string.IsNullOrEmpty(RefreshToken))
            {
                return false;
            }

            if (DateTime.Now > TokenExpires.AddHours(-1))
            {
                HttpWebRequestHelper helper = new HttpWebRequestHelper
                {
                    UserAgent = PodioHelper.USER_AGENT
                };

                var p = new Dictionary<string, string>
                {
                    ["grant_type"] = "refresh_token",
                    ["client_id"] = GetClientID(),
                    ["client_secret"] = GetClientSecret(),
                    ["refresh_token"] = RefreshToken,
                };

                var tokenResult  = helper.PostRequestAsString(p, HttpWebRequestHelper.MimeTypeApplicationWwwFormUrlEncoded, "https://podio.com/oauth/token");
                var result = HttpWebRequestHelper.FromJsonToDictionary(tokenResult);
                
                AccessToken = result["access_token"] as string;
                RefreshToken = result["refresh_token"] as string;
                TokenExpires = DateTime.Now.AddSeconds(Convert.ToInt32(result["expires_in"]));

                //Update Registry
                UpdateRegistry(_registryProvider, RegistryKey, GetRegistryInitializationParameters());
            }

            return true;
        }
       
        public override void Initialize(List<ProviderParameter> parameters)
        {
            foreach (ProviderParameter p in parameters)
            {
                AddConfigKey(p.Name, p.ConfigKey);

                switch (p.Name)
                {
                    case "RegistryKey":
                        {
                            RegistryKey = p.Value;
                            break;
                        }
                    case "Credentials":
                        {
                            Credentials = p.Value;
                            break;
                        }
                    case "ClientID":
                        {
                            ClientID = p.Value;
                            break;
                        }
                    case "ClientSecret":
                        {
                            ClientSecret = p.Value;
                            break;
                        }
                    case "AccessToken":
                        {
                            AccessToken = p.Value;
                            break;
                        }
                    case "RefreshToken":
                        {
                            RefreshToken = p.Value;
                            break;
                        }
                    case "TokenExpires":
                        {
                            DateTime dt;
                            if (DateTime.TryParse(p.Value, out dt))
                                TokenExpires = dt;
                            break;
                        }                    
                    case "SpaceID":
                        {
                            if(int.TryParse(p.Value, out var val))
                            {
                                SpaceID = val;
                            }
                            break;
                        }
                    case "Space":
                        {
                            _space = p.Value;
                            break;
                        }
                    case "SchemaName":
                        {
                            SchemaName = p.Value;
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }
            }
        }

        public override List<ProviderParameter> GetInitializationParameters()
        {
            return new List<ProviderParameter>
                       {
                           new ProviderParameter("RegistryKey", RegistryKey),
                           new ProviderParameter("Credentials", Credentials, GetConfigKey("Credentials")),
                           new ProviderParameter("ClientID", ClientID, GetConfigKey("ClientID")),
                           new ProviderParameter("ClientSecret", SecurityService.EncryptValue(ClientSecret), GetConfigKey("ClientSecret")),
                           new ProviderParameter("AccessToken", AccessToken, GetConfigKey("AccessToken")),
                           new ProviderParameter("RefreshToken", RefreshToken, GetConfigKey("RefreshToken")),
                           new ProviderParameter("TokenExpires", TokenExpires.ToString("yyyy-MM-dd HH:mm:ss"), GetConfigKey("TokenExpires")),
                           new ProviderParameter("Space", Space),
                           new ProviderParameter("SpaceID", SpaceID.ToString()),
                           new ProviderParameter("SchemaName", SchemaName)
                       };
        }
        
        internal string GetClientID() => string.IsNullOrEmpty(ClientID) ? PodioHelper.CLIENT_ID : ClientID;

        internal string GetClientSecret() => SecurityService.DecyptValue(string.IsNullOrEmpty(ClientSecret) ? PodioHelper.CLIENT_SECRET : ClientSecret);

        internal Dictionary<string, int> GetOrgSpaces()
        {
            ValidateToken();
            return Cache.GetCacheItem("Podio.SpaceTypeConverter", () => PodioHelper.GetOrgSpaces(AccessToken));
        }

        #region IDataSourceRegistry Members

        [Category("Connection.Library")]
        [Description("Key Name of the Item in the Connection Library")]
        [DisplayName("Key")]
        public string RegistryKey { get; set; }

        public void InitializeFromRegistry(IDataSourceRegistryProvider provider)
        {
            _registryProvider = provider;
            var registry = provider.Get(RegistryKey);

            if (registry != null)
            {

                foreach (ProviderParameter p in registry.Parameters)
                {
                    switch (p.Name)
                    {
                        case "Credentials":
                            {
                                Credentials = p.Value;
                                break;
                            }
                        case "ClientID":
                            {
                                ClientID = p.Value;
                                break;
                            }
                        case "ClientSecret":
                            {
                                ClientSecret = p.Value;
                                break;
                            }
                        case "AccessToken":
                            {
                                AccessToken = p.Value;
                                break;
                            }
                        case "RefreshToken":
                            {
                                RefreshToken = p.Value;
                                break;
                            }
                        case "TokenExpires":
                            {
                                DateTime dt;
                                if (DateTime.TryParse(p.Value, out dt))
                                    TokenExpires = dt;
                                break;
                            }
                        default:
                            {
                                break;
                            }
                    }
                }
            }
        }

        public IDataSourceReader ConnectFromRegistry(IDataSourceRegistryProvider provider)
        {
            InitializeFromRegistry(provider);
            return this;
        }

        public List<ProviderParameter> GetRegistryInitializationParameters()
        {
            return new List<ProviderParameter>
                       {
                           new ProviderParameter("Credentials", Credentials),
                           new ProviderParameter("ClientID", ClientID),
                           new ProviderParameter("ClientSecret", SecurityService.EncryptValue(ClientSecret)),
                           new ProviderParameter("AccessToken", AccessToken),
                           new ProviderParameter("RefreshToken", RefreshToken),
                           new ProviderParameter("TokenExpires", TokenExpires.ToString("yyyy-MM-dd HH:mm:ss"))
                       };

        }

        public object GetRegistryInterface() => this;

        #endregion        
    }
}