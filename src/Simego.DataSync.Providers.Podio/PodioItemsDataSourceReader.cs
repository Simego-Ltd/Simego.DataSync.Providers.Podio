using Newtonsoft.Json.Linq;
using Simego.DataSync.Core;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.Podio.TypeConverters;
using Simego.DataSync.Providers.Podio.TypeEditors;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Drawing.Design;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Simego.DataSync.Providers.Podio
{
    [ProviderInfo(Name = "Podio Apps", Description = "Read/Write items from Podio Apps", Group = "Podio")]
    public class PodioItemsDataSourceReader : DataReaderProviderBase, IDataSourceSetup, IDataSourceRegistry, IDataSourceRegistryView, IDataSourceLookup
    {
        private readonly ParallelExecutionOptions pOptions = new ParallelExecutionOptions();

        private ConnectionInterface _connectionIf;
        private IDataSourceRegistryProvider _registryProvider;

        private string _app;
        private string _view;

        //The Podio Data Schema
        internal PodioDataSchema PodioSchema { get; set; }

        [Description("Podio Service Credentials")]
        [Category("Service")]
        [Editor(typeof(OAuthCredentialsWebTypeEditor), typeof(UITypeEditor))]
        public string Credentials { get; set; }

        [Description("Podio Service Authentication Mode")]
        [Category("Service")]
        public PodioAuthenticationType AuthenticationMode { get; set; }

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

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal string AppAccessToken { get; set; }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal string AppRefreshToken { get; set; }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        internal DateTime AppTokenExpires { get; set; }

        [Category("Connection")]
        [Description("Do not report changes to Podio Activity Stream")]
        public bool Silent { get; set; }

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
        [TypeConverter(typeof(PodioAppTypeConverter))]
        [Description("App in Podio to connect")]
        public string App
        {
            get { return _app; }
            set
            {
                if (_app != value || AppID == 0)
                {
                    AppID = PodioHelper.GetOrgAppIdFromPath(AccessToken, AppSpaceID, value);
                }
                _app = value;
            }
        }

        [Category("Connection")]
        [TypeConverter(typeof(PodioViewTypeConverter))]
        [Description("Filtered View in Podio to connect to.")]
        public string View
        {
            get { return _view; }
            set
            {
                if (_view != value)
                {
                    var views = GetAppViews();
                    if (views.ContainsKey(value))
                        ViewID = views[value];
                }
                _view = value;
            }
        }

        [Category("Connection")]
        [Description("ID of App in Podio")]
        [ReadOnly(true)]
        public int AppID { get; set; }

        [Category("Connection")]
        [Description("App Space ID in Podio")]
        [ReadOnly(true)]
        public int AppSpaceID { get; set; }

        [Category("Connection")]
        [Description("ID of App View in Podio")]
        [ReadOnly(true)]
        public int ViewID { get; set; }

        [Category("Connection")]
        [Description("App Token in Podio")]
        [ReadOnly(true)]
        public string AppToken { get; set; }

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

        [Category("Connection")]
        [Description("DateTime Handling Utc or Local")]
        public DateTimeKind DateTimeHandling { get; set; }

        [Browsable(false)]
        public Podio Podio { get; set; }

        [Category("Connection")]
        [Description("Enable RAW Json mode where items are returned a Json document.")]
        public bool RawJsonMode { get; set; }

        public PodioItemsDataSourceReader()
        {
            AuthenticationMode = PodioAuthenticationType.Client;
            DateTimeHandling = DateTimeKind.Utc;
            TokenExpires = DateTime.Now;
            AppTokenExpires = DateTime.Now;

            Silent = true;
            Limit = 250;

            _view = "All Items";
            ViewID = 0;

            SupportsIncrementalReconciliation = true;

            Podio = new Podio(request =>
            {
                if (AuthenticationMode == PodioAuthenticationType.Client)
                {
                    ValidateToken();
                }
                else
                {
                    ValidateAppToken();
                }
                request.Headers["Authorization"] = string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken);
            });
        }

        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            if (RawJsonMode)
                return GetDataTableAsJson(dt);

            if (AuthenticationMode == PodioAuthenticationType.Client)
            {
                ValidateToken();
            }
            else
            {
                ValidateAppToken();
            }

            //In Podio "" and NULL are Equal
            dt.EmptyStringAndNullAreEqual = true;

            if (AppID == 0) throw new ArgumentOutOfRangeException(nameof(AppID), $"No Podio AppID for '{App}'.");

            //Ensure that our Schema Detail is upto date.
            if (PodioSchema == null)
                GetPodioDataSchema();

            dt.AddIdentifierColumn(typeof(long));

            DataSchemaMapping mapping = new DataSchemaMapping(SchemaMap, Side);

            int total = 0;
            int offset = 0;
            bool abort = false;
            do
            {
                string requestUrl = string.Format("https://api.podio.com/item/app/{0}/filter/?fields=items.view(micro).fields({1})", AppID, "fields,app_item_id_formatted,external_id,created_on,created_by.view(micro),last_event_on");

                HttpWebRequest webRequest = WebRequest.CreateHttp(requestUrl);
                webRequest.UserAgent = PodioHelper.USER_AGENT;
                webRequest.Method = "POST";
                webRequest.ServicePoint.Expect100Continue = false;
                webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken));
                webRequest.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;

                using (StreamWriter webRequestBody = new StreamWriter(webRequest.GetRequestStream()))
                {
                    if (ViewID > 0)
                    {
                        System.Diagnostics.Debug.WriteLine(Json.Encode(new { limit = Limit, offset = offset, view_id = ViewID, sort_by = "item_id" }));
                        webRequestBody.Write(Json.Encode(new { limit = Limit, offset = offset, view_id = ViewID, sort_by = "item_id" }));
                    }
                    else
                    {
                        webRequestBody.Write(Json.Encode(new { limit = Limit, offset = offset }));
                    }
                }

                using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                {
                    UpdateRateLimits(response, 2);

                    using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                    {
                        string s = sr.ReadToEnd();

                        dynamic result = Json.Decode(s);

                        total = ViewID > 0 ? PodioDataSchemaTypeConverter.ConvertToInvariant<int>(result["filtered"]) : PodioDataSchemaTypeConverter.ConvertToInvariant<int>(result["total"]);

                        foreach (var item_row in result["items"])
                        {
                            // Skip any NULL rows (Bug in Podio shouldn't return NULL rows.)
                            if (item_row == null) continue;

                            var newRow = dt.NewRow();
                            foreach (DataSchemaItem item in SchemaMap.GetIncludedColumns())
                            {
                                string columnName = mapping.MapColumnToDestination(item);

                                if (!PodioSchema.Columns.ContainsKey(columnName))
                                    throw new InvalidOperationException(string.Format("Column name '{0}' not found in Podio App.", columnName));

                                PodioDataSchemaItem columnInfo = PodioSchema.Columns[columnName];

                                if (columnInfo.IsRoot)
                                {
                                    if (columnInfo.IsRootSubValue)
                                    {
                                        var val = item_row[columnInfo.Name];
                                        if (val != null && val.ContainsKey(columnInfo.SubName))
                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                    }
                                    else
                                    {
                                        if (item_row.ContainsKey(columnInfo.Name))
                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(item_row[columnInfo.Name], columnInfo.PodioDataType, item.DataType);

                                    }
                                    continue;

                                }

                                foreach (var field in item_row["fields"])
                                {
                                    if (field.ContainsKey("external_id") && field["external_id"] == columnInfo.Name)
                                    {
                                        if (columnInfo.IsSubValue)
                                        {
                                            if (columnInfo.IsMultiValue && field.ContainsKey("values") && field["values"].Length > 0)
                                            {
                                                var values = new object[field["values"].Length];
                                                for (int i = 0; i < field["values"].Length; i++)
                                                {
                                                    if (field["values"][i].ContainsKey("value"))
                                                    {
                                                        dynamic val = field["values"][i]["value"];
                                                        if (val != null && val.ContainsKey(columnInfo.SubName))
                                                        {
                                                            values[i] = val[columnInfo.SubName];
                                                        }
                                                    }
                                                    else if (field["values"][i].ContainsKey("embed"))
                                                    {
                                                        dynamic val = field["values"][i]["embed"];
                                                        if (val != null && val.ContainsKey(columnInfo.SubName))
                                                        {
                                                            values[i] = val[columnInfo.SubName];
                                                        }
                                                    }
                                                }

                                                newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(values, columnInfo.PodioDataType, item.DataType);

                                            }
                                            else
                                            {
                                                switch (columnInfo.PodioDataType)
                                                {
                                                    case PodioDataSchemaItemType.Question:
                                                    case PodioDataSchemaItemType.Category:
                                                    case PodioDataSchemaItemType.Contact:
                                                        {
                                                            if (field.ContainsKey("values") && field["values"].Length > 0)
                                                            {
                                                                if (field["values"][0].ContainsKey("value"))
                                                                {
                                                                    dynamic val = field["values"][0]["value"];
                                                                    if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                                                }
                                                            }
                                                            break;
                                                        }
                                                    case PodioDataSchemaItemType.DateTime:
                                                        {
                                                            if (field.ContainsKey("values") && field["values"].Length > 0)
                                                            {
                                                                dynamic val = field["values"][0];
                                                                if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                {
                                                                    var utcTimeIsNull = val[columnInfo.TimeUtcField];

                                                                    if (utcTimeIsNull == null)
                                                                    {
                                                                        var date = DateTime.SpecifyKind(PodioDataSchemaTypeConverter.ConvertTo<DateTime>(val[columnInfo.SubName], columnInfo.PodioDataType), DateTimeHandling);
                                                                        newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(date, columnInfo.PodioDataType, item.DataType);
                                                                    }
                                                                    else
                                                                    {
                                                                        var date = DateTime.SpecifyKind(PodioDataSchemaTypeConverter.ConvertTo<DateTime>(val[columnInfo.SubName], columnInfo.PodioDataType), DateTimeKind.Utc);

                                                                        if (columnInfo.TimeDisabled || DateTimeHandling == DateTimeKind.Utc)
                                                                        {
                                                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(date, columnInfo.PodioDataType, item.DataType);
                                                                        }
                                                                        else
                                                                        {
                                                                            //Convert UTC to LocalTime
                                                                            newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(date.ToLocalTime(), columnInfo.PodioDataType, item.DataType);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            break;
                                                        }
                                                    case PodioDataSchemaItemType.Phone:
                                                    case PodioDataSchemaItemType.Email:
                                                        {
                                                            if (field.ContainsKey("values") && field["values"].Length > 0)
                                                            {
                                                                var values = new List<string>();

                                                                foreach (dynamic val in field["values"])
                                                                {
                                                                    if (val != null && val.ContainsKey("type") && val.ContainsKey("value"))
                                                                    {
                                                                        if (val["type"] == columnInfo.SubName)
                                                                        {
                                                                            values.Add(val["value"]);
                                                                        }
                                                                    }

                                                                }

                                                                if (values.Count > 0)
                                                                {
                                                                    newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(string.Join(";", values), columnInfo.PodioDataType, item.DataType);
                                                                }
                                                            }

                                                            break;
                                                        }

                                                    default:
                                                        {
                                                            if (field.ContainsKey("values") && field["values"].Length > 0)
                                                            {
                                                                dynamic val = field["values"][0];
                                                                if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                    newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                                            }

                                                            break;
                                                        }
                                                }
                                            }

                                        }
                                        else
                                        {
                                            if (field.ContainsKey("values") && field["values"].Length > 0)
                                            {
                                                dynamic val = field["values"][0];
                                                if (val != null && val.ContainsKey("value"))
                                                    newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val["value"], columnInfo.PodioDataType, item.DataType);
                                            }
                                        }
                                    }
                                }
                            }

                            if (dt.Rows.AddWithIdentifier(newRow, PodioDataSchemaTypeConverter.ConvertToInvariant<long>(item_row["item_id"])) == DataTableStore.ABORT)
                            {
                                abort = true;
                                return dt;
                            }
                        }

                        offset += result["items"].Length;

                        //If this request returns zero records assume we hit the end and quit.
                        if (result["items"].Length == 0)
                            offset = total;
                    }
                }


            } while (!abort && offset < total);

            return dt;
        }

        public override DataTableStore GetDataTable(DataTableStore dt, DataTableKeySet keyset)
        {
            // Composite Keyset not implemented.
            if (keyset.IsCompositeKeySet) return base.GetDataTable(dt, keyset);
            
            if (RawJsonMode)
                throw new NotSupportedException($"{nameof(RawJsonMode)} not supported in Incremental Mode.");

            if (Limit > 250)
                throw new ArgumentOutOfRangeException(nameof(Limit), Limit,
                    "The Maximum items you can return in Incremental mode is 250.");

            dt.AddIdentifierColumn(typeof(long));

            if (keyset.KeyValues.Any())
            {

                if (AuthenticationMode == PodioAuthenticationType.Client)
                {
                    ValidateToken();
                }
                else
                {
                    ValidateAppToken();
                }
                
                //In Podio "" and NULL are Equal
                dt.EmptyStringAndNullAreEqual = true;

                //Ensure that our Schema Detail is upto date.
                if (PodioSchema == null)
                    GetPodioDataSchema();


                DataSchemaMapping mapping = new DataSchemaMapping(SchemaMap, Side);

                //Get Data in Pages of 250
                var keycolumn = mapping.MapColumnToDestination(keyset.KeyColumn);
                var podioKeyColumn = PodioSchema.Columns[keycolumn];
                var podioKeyColumnType = podioKeyColumn.DataType;

                if (podioKeyColumn.Name != "external_id" && podioKeyColumn.Name != "item_id" && podioKeyColumn.Name != "app_item_id")
                    throw new ArgumentException("Due to Podio API limitations Incremental Sync is only supported with Podio 'external_id, item_id, app_item_id' fields.");

                if (podioKeyColumn.Name == "item_id") podioKeyColumnType = typeof(long);
                if (podioKeyColumn.Name == "app_item_id") podioKeyColumnType = typeof(int);
                if (podioKeyColumn.Name == "external_id") podioKeyColumnType = typeof(string);

                Parallel.ForEach(keyset.KeyValues.ChunkList(Limit),
                                 new ParallelOptions() { MaxDegreeOfParallelism = pOptions.MaxThreads },
                                 (chunk, loopState, index) =>
                                 {
                                     try
                                     {
                                         HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/item/app/{0}/filter/?fields=items.view(micro).fields({1})", AppID, "fields,app_item_id_formatted,external_id,created_on,created_by.view(micro),last_event_on"));
                                         webRequest.UserAgent = PodioHelper.USER_AGENT;
                                         webRequest.Method = "POST";
                                         webRequest.ServicePoint.Expect100Continue = false;
                                         webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken));

                                         using (StreamWriter webRequestBody = new StreamWriter(webRequest.GetRequestStream()))
                                         {
                                             // We are requesting a 2xchunk size here because if the data has duplicates it would not return the last items causing an incorrect result.
                                             // This is not perfect but should cover most scenarios where there are duplicate external id's

                                             var r = new { limit = chunk.Count() * 2, filters = new Dictionary<string, object>() };
                                             r.filters.Add(podioKeyColumn.Name, chunk.Select(p => PodioDataSchemaTypeConverter.ConvertTo(p, podioKeyColumn.PodioDataType, podioKeyColumnType))); //Convert to strings since podio requires External ID to be a string ....
                                             webRequestBody.Write(Json.Encode(r));
                                         }

                                         using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                                         {
                                             UpdateRateLimits(response, 2);

                                             using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                                             {
                                                 dynamic result = Json.Decode(sr.ReadToEnd());

                                                 foreach (var item_row in result["items"])
                                                 {
                                                     // Skip any NULL rows (Bug in Podio shouldn't return NULL rows.)
                                                     if (item_row == null) continue;
                                                     
                                                     var newRow = dt.NewRow();
                                                     foreach (DataSchemaItem item in SchemaMap.GetIncludedColumns())
                                                     {
                                                         string columnName = mapping.MapColumnToDestination(item);

                                                         if (!PodioSchema.Columns.ContainsKey(columnName))
                                                             throw new InvalidOperationException(string.Format("Column name '{0}' not found in Podio App.", columnName));

                                                         PodioDataSchemaItem columnInfo = PodioSchema.Columns[columnName];

                                                         if (columnInfo.IsRoot)
                                                         {
                                                             if (columnInfo.IsRootSubValue)
                                                             {
                                                                 var val = item_row[columnInfo.Name];
                                                                 if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                     newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                                             }
                                                             else
                                                             {
                                                                 if (item_row.ContainsKey(columnInfo.Name))
                                                                     newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(item_row[columnInfo.Name], columnInfo.PodioDataType, item.DataType);

                                                             }
                                                             continue;

                                                         }

                                                         foreach (var field in item_row["fields"])
                                                         {
                                                             if (field.ContainsKey("external_id") && field["external_id"] == columnInfo.Name)
                                                             {
                                                                 if (columnInfo.IsSubValue)
                                                                 {
                                                                     if (columnInfo.IsMultiValue && field.ContainsKey("values") && field["values"].Length > 0)
                                                                     {
                                                                         var values = new object[field["values"].Length];
                                                                         for (int i = 0; i < field["values"].Length; i++)
                                                                         {
                                                                             if (field["values"][i].ContainsKey("value"))
                                                                             {
                                                                                 dynamic val = field["values"][i]["value"];
                                                                                 if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                                 {
                                                                                     values[i] = val[columnInfo.SubName];
                                                                                 }
                                                                             }
                                                                             else if (field["values"][i].ContainsKey("embed"))
                                                                             {
                                                                                 dynamic val = field["values"][i]["embed"];
                                                                                 if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                                 {
                                                                                     values[i] = val[columnInfo.SubName];
                                                                                 }
                                                                             }
                                                                         }

                                                                         newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(values, columnInfo.PodioDataType, item.DataType);

                                                                     }
                                                                     else
                                                                     {
                                                                         switch (columnInfo.PodioDataType)
                                                                         {
                                                                             case PodioDataSchemaItemType.Question:
                                                                             case PodioDataSchemaItemType.Category:
                                                                             case PodioDataSchemaItemType.Contact:
                                                                                 {
                                                                                     if (field.ContainsKey("values") && field["values"].Length > 0)
                                                                                     {
                                                                                         if (field["values"][0].ContainsKey("value"))
                                                                                         {
                                                                                             dynamic val = field["values"][0]["value"];
                                                                                             if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                                                 newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                                                                         }
                                                                                     }
                                                                                     break;
                                                                                 }
                                                                             case PodioDataSchemaItemType.DateTime:
                                                                                 {
                                                                                     if (field.ContainsKey("values") && field["values"].Length > 0)
                                                                                     {
                                                                                         dynamic val = field["values"][0];
                                                                                         if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                                         {
                                                                                             if (DateTimeHandling == DateTimeKind.Utc)
                                                                                             {
                                                                                                 newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(
                                                                                                     DateTime.SpecifyKind(PodioDataSchemaTypeConverter.ConvertTo<DateTime>(val[columnInfo.SubName], columnInfo.PodioDataType),
                                                                                                                          DateTimeKind.Utc), columnInfo.PodioDataType,
                                                                                                     item.DataType);
                                                                                             }
                                                                                             else
                                                                                             {
                                                                                                 //Convert UTC to LocalTime
                                                                                                 newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(
                                                                                                     DateTime.SpecifyKind(PodioDataSchemaTypeConverter.ConvertTo<DateTime>(val[columnInfo.SubName], columnInfo.PodioDataType),
                                                                                                                          DateTimeKind.Utc)
                                                                                                             .ToLocalTime(), columnInfo.PodioDataType,
                                                                                                     item.DataType);

                                                                                             }
                                                                                         }
                                                                                     }
                                                                                     break;
                                                                                 }
                                                                             case PodioDataSchemaItemType.Phone:
                                                                             case PodioDataSchemaItemType.Email:
                                                                                 {
                                                                                     if (field.ContainsKey("values") && field["values"].Length > 0)
                                                                                     {
                                                                                         var values = new List<string>();

                                                                                         foreach (dynamic val in field["values"])
                                                                                         {
                                                                                             if (val != null && val.ContainsKey("type") && val.ContainsKey("value"))
                                                                                             {
                                                                                                 if (val["type"] == columnInfo.SubName)
                                                                                                 {
                                                                                                     values.Add(val["value"]);
                                                                                                 }
                                                                                             }

                                                                                         }

                                                                                         if (values.Count > 0)
                                                                                         {
                                                                                             newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(string.Join(";", values), columnInfo.PodioDataType, item.DataType);
                                                                                         }
                                                                                     }

                                                                                     break;
                                                                                 }
                                                                             default:
                                                                                 {
                                                                                     if (field.ContainsKey("values") && field["values"].Length > 0)
                                                                                     {
                                                                                         dynamic val = field["values"][0];
                                                                                         if (val != null && val.ContainsKey(columnInfo.SubName))
                                                                                             newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val[columnInfo.SubName], columnInfo.PodioDataType, item.DataType);
                                                                                     }

                                                                                     break;
                                                                                 }
                                                                         }
                                                                     }

                                                                 }
                                                                 else
                                                                 {
                                                                     if (field.ContainsKey("values") && field["values"].Length > 0)
                                                                     {
                                                                         dynamic val = field["values"][0];
                                                                         if (val != null && val.ContainsKey("value"))
                                                                             newRow[item.ColumnName] = PodioDataSchemaTypeConverter.ConvertTo(val["value"], columnInfo.PodioDataType, item.DataType);
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }

                                                     if (dt.Rows.AddWithIdentifier(newRow, PodioDataSchemaTypeConverter.ConvertToInvariant<long>(item_row["item_id"])) == DataTableStore.ABORT)
                                                     {
                                                         loopState.Break();
                                                         break;
                                                     }
                                                 }


                                             }
                                         }
                                     }
                                     catch (WebException e)
                                     {
                                         if (e != null && e.Response != null)
                                         {
                                             var stream = e.Response.GetResponseStream();
                                             if (stream != null && stream.CanRead)
                                             {
                                                 using (StreamReader sr = new StreamReader(stream))
                                                 {
                                                     throw new WebException(string.Format("{0}\r\n\r\n{1}", e.Message, sr.ReadToEnd()), e);
                                                 }
                                             }

                                         }

                                         throw;
                                     }

                                 });

            }

            return dt;
        }

        private DataTableStore GetDataTableAsJson(DataTableStore dt)
        {
            if (AuthenticationMode == PodioAuthenticationType.Client)
            {
                ValidateToken();
            }
            else
            {
                ValidateAppToken();
            }

            dt.AddIdentifierColumn(typeof(long));

            //In Podio "" and NULL are Equal
            dt.EmptyStringAndNullAreEqual = true;

            if (AppID == 0) throw new ArgumentOutOfRangeException(nameof(AppID), $"No Podio AppID for '{App}'.");

            DataSchemaMapping mapping = new DataSchemaMapping(SchemaMap, Side);

            HttpWebRequestHelper helper = new HttpWebRequestHelper
            {
                UserAgent = PodioHelper.USER_AGENT,
                AuthorizationHeader = $"OAuth2 {(AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken)}"
            };

            int total = 0;
            int offset = 0;
            bool abort = false;
            do
            {
                //string requestUrl = $"https://api.podio.com/item/app/{AppID}/filter/?fields=items.view(full)";
                string requestUrl = $"https://api.podio.com/item/app/{AppID}/filter/?fields=items.view(micro).fields(fields.view(micro),title,external_id,created_on,last_event_on)";
                var view = ViewID > 0 ? (object)new { limit = Limit, offset = offset, view_id = ViewID, sort_by = "item_id" } : (object)new { limit = Limit, offset = offset };

                var result = helper.PostRequestAsJson(view, requestUrl);

                var count = result["items"].Count();

                total = PodioDataSchemaTypeConverter.ConvertToInvariant<int>(ViewID > 0 ? result["filtered"] : result["total"]);

                foreach (var item_row in result["items"])
                {
                    var id = item_row["item_id"].ToObject<long>();

                    // Remove these elements from the document as they change each time there read.
                    RemoveElementsFromDocument(item_row, new string[] { "last_seen_on" });

                    var newRow = dt.NewRow();

                    foreach (var column in SchemaMap.GetIncludedColumns())
                    {
                        string columnName = mapping.MapColumnToDestination(column);

                        switch (columnName)
                        {
                            case "item_id":
                                {
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(id, column.DataType);
                                    break;
                                }
                            case "app_id":
                                {
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(AppID, column.DataType);
                                    break;
                                }
                            case "created":
                                {
                                    var val = DateTime.SpecifyKind(item_row["created_on"].ToObject<DateTime>(), DateTimeKind.Utc);
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(val, column.DataType);
                                    break;
                                }

                            case "modified":
                                {
                                    var isValid = item_row["last_event_on"]?.ToObject<object>() != null;
                                    var val = DateTime.SpecifyKind(item_row[isValid ? "last_event_on" : "created_on"].ToObject<DateTime>(), DateTimeKind.Utc);
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(val, column.DataType);
                                    break;
                                }

                            case "title":
                                {
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(item_row["title"], column.DataType);
                                    break;
                                }
                            case "external_id":
                                {
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(item_row["external_id"], column.DataType);
                                    break;
                                }
                            case "json":
                                {
                                    newRow[column.ColumnName] = DataSchemaTypeConverter.ConvertTo(item_row, column.DataType);
                                    break;
                                }
                        }
                    }

                    if (dt.Rows.AddWithIdentifier(newRow, id) == DataTableStore.ABORT)
                    {
                        abort = true;
                        return dt;
                    }
                }

                offset += count;

                //If this request returns zero records assume we hit the end and quit.
                if (count == 0) offset = total;

            } while (!abort && offset < total);

            return dt;
        }
        
        public override DataSchema GetDefaultDataSchema()
        {
            if (RawJsonMode)
            {
                DataSchema schema = new DataSchema();

                schema.Map.Add(new DataSchemaItem("item_id", typeof(long), true, false, false, -1));
                schema.Map.Add(new DataSchemaItem("app_id", typeof(int), false, false, true, -1));
                schema.Map.Add(new DataSchemaItem("external_id", typeof(string), false, false, true, -1));
                schema.Map.Add(new DataSchemaItem("created", typeof(DateTime), false, false, true, -1));
                schema.Map.Add(new DataSchemaItem("modified", typeof(DateTime), false, false, true, -1));
                schema.Map.Add(new DataSchemaItem("title", typeof(string), false, false, true, -1));
                schema.Map.Add(new DataSchemaItem("json", typeof(JToken), false, false, true, -1));

                return schema;
            }

            ValidateToken();

            if (AppID == 0) throw new ArgumentOutOfRangeException(nameof(AppID), $"No Podio AppID for '{App}'.");

            //Get the Request Token
            var webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/app/{0}", AppID));
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AccessToken));

            using (var response = (HttpWebResponse)webRequest.GetResponse())
            {
                UpdateRateLimits(response, 1);

                using (var sr = new StreamReader(response.GetResponseStream()))
                {
                    PodioSchema = PodioDataSchema.FromJson(sr.ReadToEnd());
                    AppToken = PodioSchema.Token;
                    AppSpaceID = PodioSchema.SpaceID;
                    AppTokenExpires = DateTime.Now;
                    var schema = PodioSchema.ToDataSchema();
                    SetSchemaCache(schema);
                    return schema;
                }
            }
        }

        internal DataSchema GetPodioDataSchema()
        {
            if (AuthenticationMode == PodioAuthenticationType.Client)
            {
                ValidateToken();
            }
            else
            {
                ValidateAppToken();
            }

            if (AppID == 0) throw new ArgumentOutOfRangeException(nameof(AppID), $"No Podio AppID for '{App}'.");

            //Get the Request Token
            var webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/app/{0}", AppID));
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken));

            using (var response = (HttpWebResponse)webRequest.GetResponse())
            {
                UpdateRateLimits(response, 1);

                using (var sr = new StreamReader(response.GetResponseStream()))
                {
                    PodioSchema = PodioDataSchema.FromJson(sr.ReadToEnd());
                    AppToken = PodioSchema.Token;
                    AppSpaceID = PodioSchema.SpaceID;
                    AppTokenExpires = DateTime.Now;
                    var schema = PodioSchema.ToDataSchema();
                    SetSchemaCache(schema);
                    return schema;
                }
            }
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

        public DateTime? GetAppLastChanged(string appId)
        {
            if(int.TryParse(appId, out int val))
            {
                return GetAppLastChanged(val);
            }
            return null;
        }
        public DateTime? GetAppLastChanged(int appId)
        {
            if (AuthenticationMode == PodioAuthenticationType.Client)
            {
                ValidateToken();
            }
            else
            {
                ValidateAppToken();
            }

            HttpWebRequest webRequest = WebRequest.CreateHttp($"https://api.podio.com/item/app/{appId}/filter/?fields=items.view(micro).fields(last_event_on)");
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "POST";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken));

            using (StreamWriter webRequestBody = new StreamWriter(webRequest.GetRequestStream()))
            {
                var r = new { limit = 1, sort_by = "last_edit_on", sort_desc = true };
                webRequestBody.Write(Json.Encode(r));
            }

            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
            {         
                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic result = Json.Decode(sr.ReadToEnd());

                    foreach (var item_row in result["items"])
                    {
                        return DataSchemaTypeConverter.ConvertTo<DateTime>(item_row["last_event_on"]);
                    }
                }
            }

            return null;
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

        private bool ValidateAppToken()
        {
            if (string.IsNullOrEmpty(AppToken))
            {
                return false;
            }

            if (DateTime.Now > AppTokenExpires.AddSeconds(-30))
            {
                //Refresh the Access Token
                HttpWebRequest webRequest = WebRequest.CreateHttp("https://podio.com/oauth/token");
                webRequest.UserAgent = PodioHelper.USER_AGENT;
                webRequest.Method = "POST";
                webRequest.ServicePoint.Expect100Continue = false;

                //grant_type=app&app_id=YOUR_PODIO_APP_ID&app_token=YOUR_PODIO_APP_TOKEN&client_id=YOUR_APP_ID&redirect_uri=YOUR_URL&client_secret=YOUR_APP_SECRET
                string postData = string.Format("grant_type=app&app_id={0}&app_token={1}&client_id={2}&redirect_uri=&client_secret={3}", AppID, AppToken, GetClientID(), GetClientSecret());

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

                        AppAccessToken = result["access_token"];
                        AppRefreshToken = result["refresh_token"];
                        AppTokenExpires = DateTime.Now.AddSeconds(Convert.ToInt32(result["expires_in"]));

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
                    case "AuthenticationMode":
                        {
                            PodioAuthenticationType val;
                            if (Enum.TryParse(p.Value, true, out val))
                            {
                                AuthenticationMode = val;
                            }

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
                    case "App":
                        {
                            //Set the private backing field version so we do not discover Apps during initialization.
                            _app = p.Value;
                            break;
                        }
                    case "AppID":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                            {
                                AppID = val;
                            }
                            else
                            {
                                // Connection Library trys to set this in the form id#name
                                var sp = SplitString(p.Value);
                                if (sp.Item1 > 0)
                                {
                                    AppID = sp.Item1;
                                    // Set the App name to the second param.
                                    _app = sp.Item2;
                                }
                            }
                            break;
                        }
                    case "AppSpaceID":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                                AppSpaceID = val;
                            break;
                        }
                    case "View":
                        {
                            //Set the private backing field version so we do not discover Apps during initialization.
                            _view = p.Value;
                            break;
                        }
                    case "ViewID":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                                ViewID = val;
                            break;
                        }
                    case "AppToken":
                        {
                            AppToken = p.Value;
                            break;
                        }
                    case "Silent":
                        {
                            bool val;
                            if (bool.TryParse(p.Value, out val))
                                Silent = val;
                            break;
                        }
                    case "Limit":
                        {
                            int val;
                            if (int.TryParse(p.Value, out val))
                                Limit = val;
                            break;
                        }
                    case "DateTimeHandling":
                        {
                            DateTimeHandling = (DateTimeKind)Enum.Parse(typeof(DateTimeKind), p.Value, true);
                            break;
                        }
                    case "RawJsonMode":
                        {
                            bool val;
                            if (bool.TryParse(p.Value, out val))
                                RawJsonMode = val;
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
                           new ProviderParameter("AuthenticationMode", AuthenticationMode.ToString(), GetConfigKey("AuthenticationMode")),
                           new ProviderParameter("ClientID", ClientID, GetConfigKey("ClientID")),
                           new ProviderParameter("ClientSecret", SecurityService.EncryptValue(ClientSecret), GetConfigKey("ClientSecret")),
                           new ProviderParameter("AccessToken", AccessToken, GetConfigKey("AccessToken")),
                           new ProviderParameter("RefreshToken", RefreshToken, GetConfigKey("RefreshToken")),
                           new ProviderParameter("TokenExpires", TokenExpires.ToString("yyyy-MM-dd HH:mm:ss"), GetConfigKey("TokenExpires")),
                           new ProviderParameter("App", App, GetConfigKey("App")),
                           new ProviderParameter("AppID", AppID.ToString(CultureInfo.InvariantCulture), GetConfigKey("AppID")),
                           new ProviderParameter("AppSpaceID", AppSpaceID.ToString(CultureInfo.InvariantCulture), GetConfigKey("AppSpaceID")),
                           new ProviderParameter("View", View, GetConfigKey("View")),
                           new ProviderParameter("ViewID", ViewID.ToString(CultureInfo.InvariantCulture), GetConfigKey("ViewID")),
                           new ProviderParameter("AppToken", AppToken, GetConfigKey("AppToken")),
                           new ProviderParameter("Limit", Limit.ToString(CultureInfo.InvariantCulture), GetConfigKey("Limit")),
                           new ProviderParameter("Silent", Silent.ToString(), GetConfigKey("Silent")),
                           new ProviderParameter("DateTimeHandling", DateTimeHandling.ToString(), GetConfigKey("DateTimeHandling")),
                           new ProviderParameter("RawJsonMode", RawJsonMode.ToString(), GetConfigKey("RawJsonMode"))
                       };
        }

        public override IDataSourceWriter GetWriter()
        {
            return RawJsonMode
                ? new NullWriterDataSourceProvider() { SchemaMap = SchemaMap }
                : (IDataSourceWriter)new PodioItemsDataSourceWriter { SchemaMap = SchemaMap };
        }

        #region IDataSourceSetup Members

        public void DisplayConfigurationUI(IntPtr parent)
        {
            var parentControl = Control.FromHandle(parent);

            if (_connectionIf == null)
            {
                _connectionIf = new ConnectionInterface();
                _connectionIf.PropertyGrid.SelectedObject = new ItemsConnectionProperties(this);
            }

            _connectionIf.Font = parentControl.Font;
            _connectionIf.Size = new Size(parentControl.Width, parentControl.Height);
            _connectionIf.Location = new Point(0, 0);
            _connectionIf.Dock = System.Windows.Forms.DockStyle.Fill;

            parentControl.Controls.Add(_connectionIf);
        }

        public bool Validate()
        {
            try
            {
                if (Credentials != "Connected")
                {
                    throw new ArgumentException("You must connect to Podio.");
                }

                if (AppID <= 0)
                {
                    throw new ArgumentException("You must specify a valid Podio APP ID.");
                }

                GetDefaultDataSchema();
                return true;
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message, "Podio Reader", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }

            return false;

        }

        public IDataSourceReader GetReader()
        {
            return this;
        }

        #endregion

        internal string GetClientID()
        {
            return string.IsNullOrEmpty(ClientID) ? PodioHelper.CLIENT_ID : ClientID;
        }

        internal string GetClientSecret()
        {            
            return SecurityService.DecyptValue(string.IsNullOrEmpty(ClientSecret) ? PodioHelper.CLIENT_SECRET : ClientSecret);
        }

        internal Dictionary<string, int> GetOrgApps()
        {
            ValidateToken();
            return Cache.GetCacheItem("Podio.AppTypeConverter", () => PodioHelper.GetOrgApps(AccessToken));
        }

        internal Dictionary<string, int> GetAppViews()
        {
            ValidateToken();
            return Cache.GetCacheItem(string.Format("Podio.AppTypeConverter.{0}", AppID), () => PodioHelper.GetAppViews(AccessToken, AppID));
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

        public object GetRegistryInterface()
        {
            return string.IsNullOrEmpty(RegistryKey) ? this : (object)new PodioItemsDataSourceReaderWithRegistry(this);
        }

        #endregion

        public DataTableStore GetLookupTable(DataLookupSource source, List<string> columns)
        {
            var reader = new PodioItemsDataSourceReader
            {
                Credentials = Credentials,
                DateTimeHandling = DateTimeHandling,
                ClientID = ClientID,
                ClientSecret = ClientSecret,
                AccessToken = AccessToken,
                RefreshToken = RefreshToken,
                TokenExpires = TokenExpires,
                AppSpaceID = AppSpaceID,
                App = source.Name,
                View = source.Config.ContainsKey("View") ? source.Config["View"] : View,
                Limit = Limit
            };

            reader.Initialize(SecurityService);


            var defaultSchema = reader.GetDefaultDataSchema();
            reader.SchemaMap = new DataSchema();

            foreach (var dsi in defaultSchema.Map)
            {
                foreach (var column in columns)
                {
                    if (dsi.ColumnDisplayNameA.Equals(column))
                        reader.SchemaMap.Map.Add(dsi.Copy());
                }
            }

            return reader.GetDataTable();
        }

        public bool UpdateSourceRow(Dictionary<string, dynamic> fields, object identity)
        {
            return UpdateSourceRow(Json.Encode(fields), identity);
        }

        public bool UpdateSourceRow(string json, object identity)
        {
            var url = string.Format("https://api.podio.com/item/{0}", identity);
            if (Silent)
                url += "?silent=1";

            HttpWebRequest webRequest = WebRequest.CreateHttp(url);
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "PUT";
            webRequest.ContentType = "application/json";
            webRequest.Accept = "application/json";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", AuthenticationMode == PodioAuthenticationType.Client ? AccessToken : AppAccessToken));

            byte[] data = Encoding.UTF8.GetBytes(json);

            webRequest.ContentLength = data.Length;

            using (Stream requestStream = webRequest.GetRequestStream())
            {
                requestStream.Write(data, 0, data.Length);
            }

            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
            {
                UpdateRateLimits(response, 2);

                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic result = Json.Decode(sr.ReadToEnd());

                    return true;
                }
            }
        }        

        private void RemoveElementsFromDocument(JToken token, string[] fields)
        {
            var container = token as JContainer;
            if (container == null) return;

            List<JToken> removeList = new List<JToken>();
            foreach (JToken el in container.Children())
            {
                if (el is JProperty p && fields.Contains(p.Name))
                {
                    removeList.Add(el);
                }
                RemoveElementsFromDocument(el, fields);
            }

            foreach (var el in removeList)
            {
                el.Remove();
            }
        }

        public RegistryConnectionInfo GetRegistryConnectionInfo()
        {
            return new RegistryConnectionInfo { GroupName = "Podio Connnections", ConnectionGroupImage = RegistryImageEnum.Podio, ConnectionImage = RegistryImageEnum.Podio };
        }

        public RegistryViewContainer GetRegistryViewContainer(string parent, string id, object state)
        {
            if (AuthenticationMode == PodioAuthenticationType.Client)
            {
                ValidateToken();
            }
            else
            {
                ValidateAppToken();
            }
            
            if (parent == null)
            {
                var rootFolder = new RegistryFolderType
                {
                    Image = RegistryImageEnum.Podio,
                    Preview = false,                    
                };

                var orgs = new RegistryFolder
                {
                    FolderName = "Organizations",
                    Image = RegistryImageEnum.Folder,
                    Completed = false,
                    FolderObjectType = new RegistryFolderType { Preview = false, ParameterName = "OrgID", Image = RegistryImageEnum.PodioOrg }
                };

                var response = PodioHelper.GetOrgs(AccessToken);
                
                foreach (var key in response.Keys.OrderBy(k => k))
                {
                    orgs.AddFolderItem($"{response[key]}#{key}", key);
                }
              
                return new RegistryViewContainer(rootFolder, new[] { orgs } );
            }

            if (parent == "Organizations")
            {
                var orgSplit = SplitString(id);

                var spaces = new RegistryFolder
                {
                    FolderName = "Spaces",
                    Image = RegistryImageEnum.Folder,
                    Completed = false,
                    FolderObjectType = new RegistryFolderType { Preview = true, ParameterName = "OrgID", Image = RegistryImageEnum.PodioSpace }
                };
                
                var response = PodioHelper.GetOrgSpaces(AccessToken, orgSplit.Item1);

                foreach (var key in response.Keys.OrderBy(k => k))
                {
                    spaces.AddFolderItem($"{response[key]}#{orgSplit.Item2}\\{key}", key);
                }
                
                return new RegistryViewContainer(null, new[] { spaces });
            }
            
            if (parent == "Spaces")
            {
                var spaceSplit = SplitString(id);

                var apps = new RegistryFolder
                {
                    FolderName = "Apps",
                    Image = RegistryImageEnum.Folder,
                    Completed = true,
                    FolderObjectType = new RegistryFolderType { Preview = true, ParameterName = nameof(PodioItemsDataSourceReader.AppID), DataType = InstanceHelper.GetTypeNameString(typeof(PodioItemsDataSourceReader)), Image = RegistryImageEnum.PodioApp }
                };

                apps.FolderObjectType.AddConnectionParameter(nameof(AppSpaceID), spaceSplit.Item1.ToString());

                var response = PodioHelper.GetOrgApps(AccessToken, spaceSplit.Item1);

                foreach (var key in response.Keys.OrderBy(k => k))
                {
                    apps.AddFolderItem($"{response[key]}#{spaceSplit.Item2}\\{key}",key);
                }
               
                var contacts = new RegistryFolder
                {
                    FolderName = "Contacts",
                    Image = RegistryImageEnum.Folder,
                    Completed = true,
                    FolderObjectType = new RegistryFolderType { Preview = true, ParameterName = nameof(PodioContactsDataSourceReader.SpaceID), DataType = InstanceHelper.GetTypeNameString(typeof(PodioContactsDataSourceReader)), Image = RegistryImageEnum.PodioContact }
                };

                contacts.FolderObjectType.AddConnectionParameter(nameof(PodioContactsDataSourceReader.Space), spaceSplit.Item2);
                contacts.AddFolderItem(spaceSplit.Item1.ToString(), "Space Contacts");

                var members = new RegistryFolder
                {
                    FolderName = "Members",
                    Image = RegistryImageEnum.Folder,
                    Completed = true,
                    FolderObjectType = new RegistryFolderType { Preview = true, ParameterName = nameof(PodioMembersDataSourceReader.SpaceID), DataType = InstanceHelper.GetTypeNameString(typeof(PodioMembersDataSourceReader)), Image = RegistryImageEnum.PodioMember }
                };

                members.FolderObjectType.AddConnectionParameter(nameof(PodioMembersDataSourceReader.Space), spaceSplit.Item2);
                members.AddFolderItem(spaceSplit.Item1.ToString(), "Space Members");

                var schema = new RegistryFolder
                {
                    FolderName = "Schema",
                    Image = RegistryImageEnum.Folder,
                    Completed = true,
                    FolderObjectType = new RegistryFolderType { Preview = true, ParameterName = nameof(PodioDbSchemaDataSourceReader.SpaceID), DataType = InstanceHelper.GetTypeNameString(typeof(PodioDbSchemaDataSourceReader)), Image = RegistryImageEnum.Table }
                };

                schema.FolderObjectType.AddConnectionParameter(nameof(PodioDbSchemaDataSourceReader.Space), spaceSplit.Item2);
                schema.AddFolderItem(spaceSplit.Item1.ToString(), "DB Schema");


                return new RegistryViewContainer(null, new[] { apps, contacts, members, schema });
            }

            return null;            
        }

        public object GetRegistryViewConfigurationInterface()
        {
            return null;            
        }

        private Tuple<int, string> SplitString(string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                var pos = value.IndexOf('#');
                if (pos > 0)
                {
                    var left = value.Substring(0, pos);
                    var right = value.Substring(pos + 1);

                    if (int.TryParse(left, out var id))
                    {
                        return new Tuple<int, string>(id, right);
                    }
                }
            }

            return new Tuple<int, string>(0, value);
        }
    }

    public class PodioItemsDataSourceReaderWithRegistry : DataReaderRegistryView<PodioItemsDataSourceReader>
    {
        [Category("Connection.Library")]
        [Description("Key Name of the Item in the Connection Library")]
        [DisplayName("Key")]
        public string RegistryKey { get { return _reader.RegistryKey; } set { _reader.RegistryKey = value; } }

        [Category("Connection")]
        [Description("Do not report changes to Podio Activity Stream")]
        public bool Silent { get { return _reader.Silent; } set { _reader.Silent = value; } }

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
        [TypeConverter(typeof(PodioAppTypeConverter))]
        [Description("App in Podio to connect")]
        public string App { get { return _reader.App; } set { _reader.App = value; } }

        [Category("Connection")]
        [TypeConverter(typeof(PodioViewTypeConverter))]
        [Description("Filtered View in Podio to connect to.")]
        public string View { get { return _reader.View; } set { _reader.View = value; } }

        [Category("Connection")]
        [Description("ID of App in Podio")]
        [ReadOnly(true)]
        public int AppID { get { return _reader.AppID; } set { _reader.AppID = value; } }

        [Category("Connection")]
        [Description("App Space ID in Podio")]
        [ReadOnly(true)]
        public int AppSpaceID { get { return _reader.AppSpaceID; } set { _reader.AppSpaceID = value; } }

        [Category("Connection")]
        [Description("ID of App View in Podio")]
        [ReadOnly(true)]
        public int ViewID { get { return _reader.ViewID; } set { _reader.ViewID = value; } }

        [Category("Connection")]
        [Description("DateTime Handling Utc or Local")]
        public DateTimeKind DateTimeHandling { get { return _reader.DateTimeHandling; } set { _reader.DateTimeHandling = value; } }

        [Browsable(false)]
        public string Credentials { get { return _reader.Credentials; } set { _reader.Credentials = value; } }

        [Category("Connection")]
        [Description("Enable RAW Json mode where items are returned a Json document.")]
        public bool RawJsonMode { get { return _reader.RawJsonMode; } set { _reader.RawJsonMode = value; } }

        public PodioItemsDataSourceReaderWithRegistry(PodioItemsDataSourceReader reader)
            : base(reader)
        {

        }

        internal Dictionary<string, int> GetOrgApps()
        {
            return _reader.GetOrgApps();
        }

        internal Dictionary<string, int> GetAppViews()
        {
            return _reader.GetAppViews();
        }
    }
}
