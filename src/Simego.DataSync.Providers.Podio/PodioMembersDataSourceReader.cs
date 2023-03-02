using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.Podio.TypeConverters;
using Simego.DataSync.Providers.Podio.TypeEditors;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Drawing.Design;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;

namespace Simego.DataSync.Providers.Podio
{
    public enum PodioRoleType
    {
        Light,
        Regular,
        Admin
    }

    [ProviderInfo(Name = "Podio Members", Description = "Read/Write members from Podio Workspace", Group = "Podio")]
    [ProviderIgnore]
    public class PodioMembersDataSourceReader : DataReaderProviderBase, IDataSourceRegistry
    {
        private ConnectionInterface _connectionIf;
        private IDataSourceRegistryProvider _registryProvider;
        
        private string _space;

        //The Podio Data Schema
        internal PodioDataSchema PodioSchema { get; set; }

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
        internal DateTime TokenExpires { get; set; }

        [Category("Connection")]
        [Description("Number of Items Podio returns in each request")]
        public int Limit { get; set; }

        [Category("Podio Api")]
        [ReadOnly(true)]
        public string ApiRateLevel1 { get; set; }

        [Category("Podio Api")]
        [ReadOnly(true)]
        public string ApiRateLevel2 { get; set; }

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
                    var spaces = GetOrgSpaces();
                    if (spaces.ContainsKey(value))
                        SpaceID = spaces[value];
                }
                _space = value;
            }
        }

        [Category("Connection")]
        [Description("ID of Space in Podio")]
        [ReadOnly(true)]
        public int SpaceID { get; set; }

        [Category("Members.Writer")]
        [Description("The default role to apply to new members.")]
        public PodioRoleType Role { get; set; }

        [Category("Members.Writer")]
        [Description("The message sent to new members.")]
        [Editor(typeof(MultilineStringEditor), typeof(UITypeEditor))]
        public string Message { get; set; }

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

        [Browsable(false)]
        public Podio Podio { get; set; }

        public PodioMembersDataSourceReader()
        {
            Role = PodioRoleType.Light;
            TokenExpires = DateTime.Now;

            Limit = 250;

            Podio = new Podio(request =>
            {
                ValidateToken();
                request.Headers["Authorization"] = string.Format("OAuth2 {0}", AccessToken);
            });
        }

        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            ValidateToken();

            //In Podio "" and NULL are Equal
            dt.EmptyStringAndNullAreEqual = true;

            //Ensure that our Schema Detail is upto date.
            if (PodioSchema == null)
                GetDefaultDataSchema();

            dt.AddIdentifierColumn(typeof(long));

            DataSchemaMapping mapping = new DataSchemaMapping(SchemaMap, Side);

            int total = 0;
            int offset = 0;
            bool abort = false;
            do
            {
                // Changed to use updated member/v2 endpoint to return user roles
                // Old URL was HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/contact/space/{0}/?contact_type=user&exclude_self=false&type=mini&limit={1}&offset={2}", SpaceID, Limit, offset));

                HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/space/{0}/member/v2?limit={1}&offset={2}", SpaceID, Limit, offset));                
                webRequest.UserAgent = PodioHelper.USER_AGENT;
                webRequest.Method = "GET";
                webRequest.ServicePoint.Expect100Continue = false;
                webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AccessToken));

                using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                {
                    UpdateRateLimits(response, 1);

                    using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                    {
                        var v = sr.ReadToEnd();
                        dynamic result = Json.Decode(v);

                        //Handle Paging
                        int count = PodioDataSchemaTypeConverter.ConvertToInvariant<int>(result.Length);
                        if (count == 0) break;
                        total += count;
                        if (count == Limit)
                            total += 1;

                        foreach (var item_row in result)
                        {                            
                            var newRow = dt.NewRow();
                            foreach (DataSchemaItem item in SchemaMap.GetIncludedColumns())
                            {
                                string columnName = mapping.MapColumnToDestination(item);
                                PodioDataSchemaItem columnInfo = PodioSchema.Columns[columnName];

                                if (columnInfo.IsRoot)
                                {
                                    if (!item_row.ContainsKey(columnInfo.Name))
                                        continue;

                                    if (columnInfo.IsMultiValue)
                                    {
                                        var values = new object[item_row[columnInfo.Name].Length];
                                        for (int i = 0; i < item_row[columnInfo.Name].Length; i++)
                                        {
                                            values[i] = item_row[columnInfo.Name];
                                        }

                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(values, columnInfo.PodioDataType, item.DataType);
                                    }
                                    else if (columnInfo.IsMultiIndexValue)
                                    {
                                        if (item_row[columnInfo.Name].Length > columnInfo.Index)
                                        {
                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(item_row[columnInfo.Name][columnInfo.Index], columnInfo.PodioDataType, item.DataType);
                                        }
                                    }
                                    else
                                    {
                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(item_row[columnInfo.Name], columnInfo.PodioDataType, item.DataType);
                                    }
                                }
                                else if (columnInfo.IsSubValue)
                                {
                                    var subValue = item_row[columnInfo.SubName];

                                    if (!subValue.ContainsKey(columnInfo.Name))
                                        continue;

                                    if (columnInfo.IsMultiValue)
                                    {
                                        var values = new object[subValue[columnInfo.Name].Length];
                                        for (int i = 0; i < subValue[columnInfo.Name].Length; i++)
                                        {
                                            values[i] = subValue[columnInfo.Name];
                                        }

                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(values, columnInfo.PodioDataType, item.DataType);
                                    }
                                    else if (columnInfo.IsMultiIndexValue)
                                    {
                                        if (subValue[columnInfo.Name].Length > columnInfo.Index)
                                        {
                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(subValue[columnInfo.Name][columnInfo.Index], columnInfo.PodioDataType, item.DataType);
                                        }
                                    }
                                    else
                                    {
                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(subValue[columnInfo.Name], columnInfo.PodioDataType, item.DataType);
                                    }
                                }

                            }

                            if (dt.Rows.AddWithIdentifier(newRow, PodioDataSchemaTypeConverter.ConvertToInvariant<long>(item_row["profile"]["user_id"])) == DataTableStore.ABORT)
                            {
                                abort = true;
                                return dt;
                            }
                        }

                        offset += result.Length;
                    }
                }


            } while (!abort && offset < total);

            return dt;
        }


        public override DataSchema GetDefaultDataSchema()
        {
            PodioSchema = PodioDataSchema.Users();
            return PodioSchema.ToDataSchema();
        }

        internal void UpdateRateLimits(HttpWebResponse response, int level)
        {
            int val;
            if (int.TryParse(response.Headers["X-Rate-Limit-Limit"], out val))
            {
                if (level == 1)
                {
                    ApiRateLevel1 = val.ToString(CultureInfo.CurrentCulture);
                }
                if (level == 2)
                {
                    ApiRateLevel2 = val.ToString(CultureInfo.CurrentCulture);
                }
            }

            if (int.TryParse(response.Headers["X-Rate-Limit-Remaining"], out val))
            {
                if (level == 1)
                {
                    ApiRateLevel1 = string.Format("{0}:{1}", val, ApiRateLevel1);
                }
                if (level == 2)
                {
                    ApiRateLevel2 = string.Format("{0}:{1}", val, ApiRateLevel2);
                }
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
                //Refresh the Access Token
                HttpWebRequest webRequest = WebRequest.CreateHttp("https://podio.com/oauth/token");
                webRequest.UserAgent = PodioHelper.USER_AGENT;
                webRequest.Method = "POST";
                webRequest.ServicePoint.Expect100Continue = false;

                string postData = string.Format("grant_type=refresh_token&client_id={0}&client_secret={1}&refresh_token={2}", GetClientID(), GetClientSecret(), RefreshToken);

                byte[] data = Encoding.UTF8.GetBytes(postData);

                webRequest.ContentType = "application/x-www-form-urlencoded";
                webRequest.Accept = "application/json";
                webRequest.ContentLength = data.Length;

                using (Stream requestStream = webRequest.GetRequestStream())
                {
                    requestStream.Write(data, 0, data.Length);
                }

                using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                {
                    UpdateRateLimits(response, 1);

                    using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                    {
                        dynamic result = Json.Decode(sr.ReadToEnd());

                        AccessToken = result["access_token"];
                        RefreshToken = result["refresh_token"];
                        TokenExpires = DateTime.Now.AddSeconds(Convert.ToInt32(result["expires_in"]));

                        //Update Registry
                        UpdateRegistry(_registryProvider, RegistryKey, GetRegistryInitializationParameters());
                    }
                }
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
                    case "Space":
                        {
                            _space = p.Value;
                            break;
                        }
                    case "SpaceID":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                                SpaceID = val;
                            break;
                        }
                    case "Role":
                        {
                            PodioRoleType val;
                            if (Enum.TryParse(p.Value, true, out val))
                                Role = val;
                            break;
                        }
                    case "Message":
                        {
                            Message = p.Value;
                            break;
                        }
                    case "Limit":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                                Limit = val;
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
                           new ProviderParameter("ClientSecret", ClientSecret, GetConfigKey("ClientSecret")),
                           new ProviderParameter("AccessToken", AccessToken, GetConfigKey("AccessToken")),
                           new ProviderParameter("RefreshToken", RefreshToken, GetConfigKey("RefreshToken")),
                           new ProviderParameter("TokenExpires", TokenExpires.ToString("yyyy-MM-dd HH:mm:ss"), GetConfigKey("TokenExpires")),
                           new ProviderParameter("Space", Space, GetConfigKey("Space")),
                           new ProviderParameter("SpaceID", SpaceID.ToString(CultureInfo.InvariantCulture), GetConfigKey("SpaceID")),
                           new ProviderParameter("Role", Role.ToString(), GetConfigKey("Role")),
                           new ProviderParameter("Message", Message, GetConfigKey("Message")),
                           new ProviderParameter("Limit", Limit.ToString(CultureInfo.InvariantCulture), GetConfigKey("Limit"))
                       };
        }

        public override IDataSourceWriter GetWriter()
        {
            return new PodioMembersDataSourceWriter { SchemaMap = SchemaMap };
        }
        
        internal string GetClientID()
        {
            return string.IsNullOrEmpty(ClientID) ? PodioHelper.CLIENT_ID : ClientID;
        }

        internal string GetClientSecret()
        {
            return SecurityService.DecyptValue(string.IsNullOrEmpty(ClientSecret) ? PodioHelper.CLIENT_SECRET : ClientSecret);
        }

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
                           new ProviderParameter("ClientSecret", ClientSecret),
                           new ProviderParameter("AccessToken", AccessToken),
                           new ProviderParameter("RefreshToken", RefreshToken),
                           new ProviderParameter("TokenExpires", TokenExpires.ToString("yyyy-MM-dd HH:mm:ss"))
                       };

        }

        public object GetRegistryInterface()
        {
            return string.IsNullOrEmpty(RegistryKey) ? this : (object)new PodioMembersDataSourceReaderWithRegistry(this);
        }

        #endregion        
    }

    public class PodioMembersDataSourceReaderWithRegistry : DataReaderRegistryView<PodioMembersDataSourceReader>
    {
        [Category("Connection.Library")]
        [Description("Key Name of the Item in the Connection Library")]
        [DisplayName("Key")]
        public string RegistryKey { get { return _reader.RegistryKey; } set { _reader.RegistryKey = value; } }
       
        [Category("Connection")]
        [Description("Number of Items Podio returns in each request")]
        public int Limit { get { return _reader.Limit; } set { _reader.Limit = value; } }

        [Category("Podio Api")]
        [ReadOnly(true)]
        public string ApiRateLevel1 { get { return _reader.ApiRateLevel1; } set { _reader.ApiRateLevel1 = value; } }

        [Category("Podio Api")]
        [ReadOnly(true)]
        public string ApiRateLevel2 { get { return _reader.ApiRateLevel2; } set { _reader.ApiRateLevel2 = value; } }

        [Category("Connection")]
        [TypeConverter(typeof(PodioSpaceTypeConverter))]
        [Description("Space in Podio to connect")]
        public string Space { get { return _reader.Space; } set { _reader.Space = value; } }

        [Category("Connection")]
        [Description("ID of Space in Podio")]
        [ReadOnly(true)]
        public int SpaceID { get { return _reader.SpaceID; } set { _reader.SpaceID = value; } }

        [Category("Members.Writer")]
        [Description("The default role to apply to new members.")]
        public PodioRoleType Role { get { return _reader.Role; } set { _reader.Role = value; } }

        [Category("Members.Writer")]
        [Description("The message sent to new members.")]
        [Editor(typeof(MultilineStringEditor), typeof(UITypeEditor))]
        public string Message { get { return _reader.Message; } set { _reader.Message = value; } }

        [Browsable(false)]
        public string Credentials { get { return _reader.Credentials; } set { _reader.Credentials = value; } }

        public PodioMembersDataSourceReaderWithRegistry(PodioMembersDataSourceReader reader)
            : base(reader)
        {

        }

        internal Dictionary<string, int> GetOrgSpaces()
        {
            return _reader.GetOrgSpaces();
        }

    }
}

