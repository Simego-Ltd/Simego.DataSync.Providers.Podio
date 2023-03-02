using Simego.DataSync.Engine;
using Simego.DataSync.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Linq;

namespace Simego.DataSync.Providers.Podio
{
    public class PodioItemsDataSourceWriter : DataWriterProviderBase
    {
        private PodioItemsDataSourceReader DataSourceReader { get; set; }
        private DataSchemaMapping Mapping { get; set; }

        public override void AddItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;
                string postData = null;

                foreach (var item in items.Select(p => new DataCompareItemInvariant(p)))
                {
                    if (!status.ContinueProcessing)
                        break;

                    postData = null;
                    try
                    {
                        Automation?.BeforeAddItem(this, item, null);

                        if (item.Sync)
                        {
                            #region Add Item

                            var url = string.Format("https://api.podio.com/item/app/{0}/", DataSourceReader.AppID);
                            if (DataSourceReader.Silent)
                                url += "?silent=1";

                            HttpWebRequest webRequest = WebRequest.CreateHttp(url);
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "POST";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AuthenticationMode == PodioAuthenticationType.Client ? DataSourceReader.AccessToken : DataSourceReader.AppAccessToken));

                            var itemObject = new Dictionary<string, dynamic> { { "fields", new Dictionary<string, object>() } };

                            foreach (DataCompareColumnItem dcci in item.SourceRow)
                            {
                                if (!Mapping.ColumnMapsToDestination(dcci))
                                    continue;

                                string columnB = Mapping.MapColumnToDestination(dcci);
                                PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                                //Read-Only Column?
                                if (columnInfo.ReadOnly) continue;

                                if (columnInfo.DataType == typeof(string))
                                {
                                    if (string.IsNullOrEmpty(PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.BeforeColumnValue, columnInfo.PodioDataType)))
                                        continue;
                                }

                                if (columnInfo.IsRoot)
                                {
                                    itemObject[columnInfo.Name] = dcci.BeforeColumnValue == null
                                                                      ? null
                                                                      : PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                                    continue;
                                }


                                if (columnInfo.IsSubValue && dcci.BeforeColumnValue != null)
                                {
                                    if (columnInfo.IsMultiValue)
                                    {
                                        if (columnInfo.PodioDataType == PodioDataSchemaItemType.Category || columnInfo.PodioDataType == PodioDataSchemaItemType.Question)
                                        {
                                            if (columnInfo.SubName == "text")
                                            {
                                                if (dcci.ColumnDataType == typeof(string[]))
                                                {
                                                    List<int> values = new List<int>();
                                                    foreach (var val in ((string[])dcci.BeforeColumnValue))
                                                    {
                                                        if (columnInfo.LookupValues.ContainsKey(val))
                                                        {
                                                            values.Add(columnInfo.LookupValues[val]);
                                                        }
                                                        else
                                                        {
                                                            throw new ArgumentException(string.Format("Error: Cannot lookup [id] value in '{0}' for text value '{1}'",
                                                                                                      columnInfo.DisplayName, val));
                                                        }
                                                    }

                                                    itemObject["fields"][columnInfo.Name] = values.ToArray();
                                                }
                                                else if (columnInfo.LookupValues.ContainsKey(PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.BeforeColumnValue, columnInfo.PodioDataType)))
                                                {
                                                    itemObject["fields"][columnInfo.Name] = columnInfo.LookupValues[PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.BeforeColumnValue, columnInfo.PodioDataType)];
                                                }
                                                else
                                                {
                                                    throw new ArgumentException(string.Format("Error: Cannot lookup [id] value in '{0}' for text value '{1}'", columnInfo.DisplayName,
                                                                                              dcci.BeforeColumnValue));
                                                }
                                                continue;
                                            }
                                        }
                                        else if (columnInfo.PodioDataType == PodioDataSchemaItemType.Link)
                                        {
                                            if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                                itemObject["fields"].Add(columnInfo.Name, new Dictionary<string, dynamic>());

                                            itemObject["fields"][columnInfo.Name]["url"] = PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.AfterColumnValue, columnInfo.PodioDataType);
                                        }

                                        if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                            itemObject["fields"].Add(columnInfo.Name, PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType, columnInfo.DataType));

                                    }
                                    else
                                    {
                                        if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                            itemObject["fields"].Add(columnInfo.Name, new Dictionary<string, dynamic>());


                                        switch (columnInfo.PodioDataType)
                                        {
                                            case PodioDataSchemaItemType.Phone:
                                            case PodioDataSchemaItemType.Email:
                                                {
                                                    if (itemObject["fields"][columnInfo.Name].Count == 0)
                                                    {
                                                        itemObject["fields"][columnInfo.Name] = new List<dynamic>();

                                                        foreach (var column in item.SourceRow)
                                                        {
                                                            if (string.IsNullOrEmpty(PodioDataSchemaTypeConverter.ConvertTo<string>(column.BeforeColumnValue, columnInfo.PodioDataType)))
                                                                continue;

                                                            string columnMap = Mapping.MapColumnToDestination(column);
                                                            if (columnMap != null)
                                                            {
                                                                PodioDataSchemaItem columnMapInfo = DataSourceReader.PodioSchema.Columns[columnMap];

                                                                if (columnInfo.Name.Equals(columnMapInfo.Name))
                                                                {
                                                                    foreach (var s in PodioDataSchemaTypeConverter.ConvertTo<string[]>(column.BeforeColumnValue, columnInfo.PodioDataType))
                                                                        itemObject["fields"][columnInfo.Name].Add(new { type = columnMapInfo.SubName, value = s });
                                                                }
                                                            }
                                                        }
                                                    }

                                                    break;
                                                }

                                            case PodioDataSchemaItemType.DateTime:
                                                {
                                                    if (columnInfo.TimeDisabled)
                                                    {
                                                        itemObject["fields"][columnInfo.Name][columnInfo.SubName] = string.Format("{0:yyyy-MM-dd}",
                                                                                                                               PodioDataSchemaTypeConverter.ConvertTo<DateTime>(
                                                                                                                                   dcci.BeforeColumnValue, columnInfo.PodioDataType));
                                                    }
                                                    else
                                                    {
                                                        itemObject["fields"][columnInfo.Name][columnInfo.SubName] = string.Format("{0:yyyy-MM-dd HH:mm:ss}",
                                                                                                                                  PodioDataSchemaTypeConverter.ConvertTo<DateTime>(
                                                                                                                                      dcci.BeforeColumnValue, columnInfo.PodioDataType).ToUniversalTime());
                                                    }

                                                    break;
                                                }
                                            default:
                                                {
                                                    itemObject["fields"][columnInfo.Name][columnInfo.SubName] = PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType,
                                                                                                                                                       columnInfo.DataType);

                                                    break;
                                                }
                                        }
                                    }
                                }
                                else
                                {
                                    if (!columnInfo.IsSubValue)
                                    {
                                        itemObject["fields"][columnInfo.Name] = PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                                    }
                                }
                            }

                            //Apply any Dependent Values
                            ApplyDependencies(itemObject["fields"], item);

                            postData = Json.Encode(itemObject);

                            byte[] data = Encoding.UTF8.GetBytes(postData);

                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ContentLength = data.Length;

                            using (Stream requestStream = webRequest.GetRequestStream())
                            {
                                requestStream.Write(data, 0, data.Length);
                            }

                            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                            {
                                DataSourceReader.UpdateRateLimits(response, 2);

                                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                                {
                                    dynamic result = Json.Decode(sr.ReadToEnd());

                                    Automation?.AfterAddItem(this, item, PodioDataSchemaTypeConverter.ConvertToInvariant<long>(result["item_id"]));
                                }
                            }

                            #endregion                            
                        }


                    }
                    catch (WebException e)
                    {
                        Automation?.ErrorItem(this, item, null, e);
                        PodioHelper.HandleError(status, postData, e);

                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem);
                    }

                }
            }
        }

        public override void UpdateItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;
                string postData = null;

                foreach (var item in items.Select(p => new DataCompareItemInvariant(p)))
                {
                    if (!status.ContinueProcessing)
                        break;

                    var entityIDValue = item.GetTargetIdentifier<long>();
                        
                    postData = null;
                    try
                    {
                        
                        Automation?.BeforeUpdateItem(this, item, entityIDValue);

                        if (item.Sync)
                        {
                            #region Update Item
                            
                            var url = string.Format("https://api.podio.com/item/{0}", entityIDValue);
                            if (DataSourceReader.Silent)
                                url += "?silent=1";

                            HttpWebRequest webRequest = WebRequest.CreateHttp(url);
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "PUT";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AuthenticationMode == PodioAuthenticationType.Client ? DataSourceReader.AccessToken : DataSourceReader.AppAccessToken));

                            var itemObject = new Dictionary<string, dynamic> { { "fields", new Dictionary<string, object>() } };

                            foreach (DataCompareColumnItem dcci in item.Row)
                            {
                                if (!Mapping.ColumnMapsToDestination(dcci))
                                    continue;

                                string columnB = Mapping.MapColumnToDestination(dcci);
                                PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                                //Read-Only Column?
                                if (columnInfo.ReadOnly) continue;

                                //Do not include self reference
                                if (columnB == "item_id")
                                    continue;

                                if (columnInfo.IsRoot)
                                {
                                    itemObject[columnInfo.Name] = dcci.AfterColumnValue == null ? null : PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                                    continue;
                                }

                                if (columnInfo.IsSubValue && dcci.AfterColumnValue != null)
                                {
                                    if (columnInfo.IsMultiValue)
                                    {
                                        if (columnInfo.PodioDataType == PodioDataSchemaItemType.Category || columnInfo.PodioDataType == PodioDataSchemaItemType.Question)
                                        {
                                            if (columnInfo.SubName == "text")
                                            {
                                                if (dcci.ColumnDataType == typeof(string[]))
                                                {
                                                    List<int> values = new List<int>();
                                                    foreach (var val in ((string[])dcci.AfterColumnValue))
                                                    {
                                                        if (columnInfo.LookupValues.ContainsKey(val))
                                                        {
                                                            values.Add(columnInfo.LookupValues[val]);
                                                        }
                                                        else
                                                        {
                                                            throw new ArgumentException(string.Format("Error: Cannot lookup [id] value in '{0}' for text value '{1}'",
                                                                                                      columnInfo.DisplayName, val));
                                                        }
                                                    }

                                                    itemObject["fields"][columnInfo.Name] = values.ToArray();
                                                }
                                                else if (columnInfo.LookupValues.ContainsKey(PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.AfterColumnValue, columnInfo.PodioDataType)))
                                                {
                                                    itemObject["fields"][columnInfo.Name] = columnInfo.LookupValues[PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.AfterColumnValue, columnInfo.PodioDataType)];
                                                }
                                                else
                                                {
                                                    throw new ArgumentException(string.Format("Error: Cannot lookup [id] value in '{0}' for text value '{1}'", columnInfo.DisplayName,
                                                                                              dcci.AfterColumnValue));
                                                }
                                                continue;
                                            }
                                        }
                                        else if (columnInfo.PodioDataType == PodioDataSchemaItemType.Link)
                                        {
                                            if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                                itemObject["fields"].Add(columnInfo.Name, new Dictionary<string, dynamic>());

                                            itemObject["fields"][columnInfo.Name]["url"] = PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.AfterColumnValue, columnInfo.PodioDataType);
                                        }

                                        if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                            itemObject["fields"].Add(columnInfo.Name, PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType));
                                    }
                                    else
                                    {
                                        if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                            itemObject["fields"].Add(columnInfo.Name, new Dictionary<string, dynamic>());


                                        switch (columnInfo.PodioDataType)
                                        {
                                            case PodioDataSchemaItemType.Phone:
                                            case PodioDataSchemaItemType.Email:
                                                {
                                                    if (itemObject["fields"][columnInfo.Name].Count == 0)
                                                    {
                                                        itemObject["fields"][columnInfo.Name] = new List<dynamic>();

                                                        foreach (var column in item.SourceRow)
                                                        {
                                                            if (string.IsNullOrEmpty(PodioDataSchemaTypeConverter.ConvertTo<string>(column.AfterColumnValue, columnInfo.PodioDataType)))
                                                                continue;

                                                            string columnMap = Mapping.MapColumnToDestination(column);
                                                            if (columnMap != null)
                                                            {
                                                                PodioDataSchemaItem columnMapInfo = DataSourceReader.PodioSchema.Columns[columnMap];

                                                                if (columnInfo.Name.Equals(columnMapInfo.Name))
                                                                {
                                                                    foreach (var s in PodioDataSchemaTypeConverter.ConvertTo<string[]>(column.AfterColumnValue, columnInfo.PodioDataType))
                                                                        itemObject["fields"][columnInfo.Name].Add(new { type = columnMapInfo.SubName, value = s });
                                                                }
                                                            }
                                                        }
                                                    }

                                                    break;
                                                }
                                            case PodioDataSchemaItemType.Location:
                                                {
                                                    if (itemObject["fields"][columnInfo.Name].Count == 0)
                                                    {
                                                        itemObject["fields"][columnInfo.Name] = new Dictionary<string, dynamic>();

                                                        foreach (var column in item.SourceRow)
                                                        {
                                                            if (string.IsNullOrEmpty(PodioDataSchemaTypeConverter.ConvertTo<string>(column.AfterColumnValue, columnInfo.PodioDataType)))
                                                                continue;

                                                            string columnMap = Mapping.MapColumnToDestination(column);
                                                            if (columnMap != null)
                                                            {
                                                                PodioDataSchemaItem columnMapInfo = DataSourceReader.PodioSchema.Columns[columnMap];

                                                                if (columnInfo.Name.Equals(columnMapInfo.Name))
                                                                {
                                                                    itemObject["fields"][columnInfo.Name].Add(columnMapInfo.SubName, column.AfterColumnValue);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    break;
                                                }
                                            case PodioDataSchemaItemType.DateTime:
                                                {

                                                    if (columnInfo.TimeDisabled)
                                                    {
                                                        itemObject["fields"][columnInfo.Name][columnInfo.SubName] = string.Format("{0:yyyy-MM-dd}",
                                                                                                                               PodioDataSchemaTypeConverter.ConvertTo<DateTime>(
                                                                                                                                   dcci.AfterColumnValue, columnInfo.PodioDataType));
                                                    }
                                                    else
                                                    {
                                                        itemObject["fields"][columnInfo.Name][columnInfo.SubName] = string.Format("{0:yyyy-MM-dd HH:mm:ss}",
                                                                                                                                 PodioDataSchemaTypeConverter.ConvertTo<DateTime>(
                                                                                                                                     dcci.AfterColumnValue, columnInfo.PodioDataType).ToUniversalTime());
                                                    }

                                                    break;
                                                }
                                            default:
                                                {
                                                    itemObject["fields"][columnInfo.Name][columnInfo.SubName] = PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType,
                                                                                                                                                       columnInfo.DataType);

                                                    break;
                                                }
                                        }
                                    }
                                }
                                else
                                {

                                    if (columnInfo.IsSubValue)
                                    {
                                        //Trying to clear a multi-value item requires setting the value null
                                        if (dcci.AfterColumnValue == null && columnInfo.Dependencies.Count(p => p.Required) == 0)
                                        {
                                            itemObject["fields"][columnInfo.Name] = null;
                                        }
                                        else
                                        {
                                            if (!itemObject["fields"].ContainsKey(columnInfo.Name))
                                                itemObject["fields"][columnInfo.Name] = new Dictionary<string, dynamic>();

                                            if (itemObject["fields"][columnInfo.Name] != null)
                                                itemObject["fields"][columnInfo.Name][columnInfo.SubName] = PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                                        }
                                    }
                                    else
                                    {
                                        itemObject["fields"][columnInfo.Name] = PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                                    }

                                }
                            }

                            //Apply any Dependent Values
                            ApplyDependencies(itemObject["fields"], item);

                            postData = Json.Encode(itemObject);

                            byte[] data = Encoding.UTF8.GetBytes(postData);

                            webRequest.ContentLength = data.Length;

                            using (Stream requestStream = webRequest.GetRequestStream())
                            {
                                requestStream.Write(data, 0, data.Length);
                            }

                            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                            {
                                DataSourceReader.UpdateRateLimits(response, 2);

                                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                                {
                                    dynamic result = Json.Decode(sr.ReadToEnd());

                                    Automation?.AfterUpdateItem(this, item, entityIDValue);
                                }
                            }

                            #endregion
                        }
                    }
                    catch (WebException e)
                    {
                        Automation?.ErrorItem(this, item, entityIDValue, e);
                        PodioHelper.HandleError(status, postData, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem);
                    }
                }
            }
        }

        public override void DeleteItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var item in items.Select(p => new DataCompareItemInvariant(p)))
                {
                    if (!status.ContinueProcessing)
                        break;

                    var entityIDValue = item.GetTargetIdentifier<long>();
                        
                    try
                    {
                        Automation?.BeforeDeleteItem(this, item, entityIDValue);

                        if (item.Sync)
                        {
                            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/item/{0}", entityIDValue));
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "DELETE";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AuthenticationMode == PodioAuthenticationType.Client ? DataSourceReader.AccessToken : DataSourceReader.AppAccessToken));

                            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                            {
                                DataSourceReader.UpdateRateLimits(response, 2);

                                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                                {
                                    dynamic result = Json.Decode(sr.ReadToEnd());

                                    Automation?.AfterDeleteItem(this, item, entityIDValue);
                                }
                            }
                        }
                    }
                    catch (WebException e)
                    {
                        Automation?.ErrorItem(this, item, entityIDValue, e);
                        PodioHelper.HandleError(status, null, e);                    
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem);
                    }

                }
            }
        }

        public override void Execute(List<DataCompareItem> addItems, List<DataCompareItem> updateItems, List<DataCompareItem> deleteItems, IDataSourceReader reader,
                                     IDataSynchronizationStatus status)
        {
            DataSourceReader = reader as PodioItemsDataSourceReader;

            if (DataSourceReader != null)
            {
                Mapping = new DataSchemaMapping(SchemaMap, DataCompare);

                //Process the Changed Items
                if (addItems != null && status.ContinueProcessing) AddItems(addItems, status);
                if (updateItems != null && status.ContinueProcessing) UpdateItems(updateItems, status);
                if (deleteItems != null && status.ContinueProcessing) DeleteItems(deleteItems, status);

            }
        }

        private void ApplyDependencies(Dictionary<string, dynamic> fields, DataCompareItemInvariant item)
        {
            //Go through Fields List
            foreach (var key in fields.Keys)
            {
                foreach (var columnInfo in DataSourceReader.PodioSchema.Columns)
                {
                    if (columnInfo.Value.Name == key && columnInfo.Value.Dependencies != null && columnInfo.Value.Dependencies.Count > 0)
                    {
                        //Validate the field dependencies
                        if (fields[key] is Dictionary<string, dynamic>)
                        {
                            foreach (var depedency in columnInfo.Value.Dependencies)
                            {
                                var dependencyColumnInfo = DataSourceReader.PodioSchema.Columns[depedency.Name];

                                if (!fields[key].ContainsKey(DataSourceReader.PodioSchema.Columns[depedency.Name].SubName))
                                {
                                    //Find the dependent value in the source
                                    foreach (var source in item.SourceRow)
                                    {                                       
                                        if (Mapping.ColumnMapsToDestination(source))
                                        {
                                            string sourceColumn = Mapping.MapColumnToDestination(source);
                                            PodioDataSchemaItem sourceColumnInfo = DataSourceReader.PodioSchema.Columns[sourceColumn];
                                            if (sourceColumnInfo.Name == dependencyColumnInfo.Name &&
                                                sourceColumnInfo.SubName == dependencyColumnInfo.SubName &&
                                                sourceColumnInfo.PodioDataType == dependencyColumnInfo.PodioDataType)
                                            {
                                                //Add this Dependent Value to the Dictionary.
                                                switch (sourceColumnInfo.DataType.ToString())
                                                {
                                                    case "System.DateTime":
                                                        {

                                                            if (source.BeforeColumnValue == null)
                                                            {
                                                                fields[key][dependencyColumnInfo.SubName] = null;
                                                            }
                                                            else
                                                            {
                                                                if (sourceColumnInfo.TimeDisabled)
                                                                {
                                                                    fields[key][dependencyColumnInfo.SubName] = string.Format("{0:yyyy-MM-dd}", PodioDataSchemaTypeConverter.ConvertTo<DateTime>(source.BeforeColumnValue, dependencyColumnInfo.PodioDataType));
                                                                }
                                                                else
                                                                {
                                                                    fields[key][dependencyColumnInfo.SubName] = string.Format("{0:yyyy-MM-dd HH:mm:ss}", PodioDataSchemaTypeConverter.ConvertTo<DateTime>(source.BeforeColumnValue, dependencyColumnInfo.PodioDataType).ToUniversalTime());
                                                                }
                                                            }
                                                            break;
                                                        }
                                                    default:
                                                        {
                                                            fields[key][dependencyColumnInfo.SubName] = PodioDataSchemaTypeConverter.ConvertTo(source.BeforeColumnValue, dependencyColumnInfo.PodioDataType, sourceColumnInfo.DataType);
                                                            break;
                                                        }
                                                }

                                                break;
                                            }
                                        }

                                    }

                                }

                                //If this dependency is required and it's not set throw an error!
                                if (depedency.Required && !fields[key].ContainsKey(dependencyColumnInfo.SubName))
                                {
                                    throw new ArgumentException(string.Format("Error: Field '{0}|{1}' has a dependency on values from '{2}|{3}' which is not part of your schema map.", key, columnInfo.Value.SubName, dependencyColumnInfo.Name, dependencyColumnInfo.SubName));
                                }
                            }
                        }

                    }

                }

            }
        }

    }
}



