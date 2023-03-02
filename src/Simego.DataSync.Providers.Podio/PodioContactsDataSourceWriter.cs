using Simego.DataSync.Engine;
using Simego.DataSync.Interfaces;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Simego.DataSync.Providers.Podio
{
    public class PodioContactsDataSourceWriter : DataWriterProviderBase
    {
        private PodioContactsDataSourceReader DataSourceReader { get; set; }
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

                           var url = string.Format("https://api.podio.com/contact/space/{0}/", DataSourceReader.SpaceID);
                           if (DataSourceReader.Silent)
                               url += "?silent=1";

                           HttpWebRequest webRequest = WebRequest.CreateHttp(url);
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "POST";
                           webRequest.ContentType = "application/json";
                           webRequest.Accept = "application/json";
                           webRequest.ServicePoint.Expect100Continue = false;
                           webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

                           var itemObject = new Dictionary<string, dynamic>();

                           foreach (DataCompareColumnItem dcci in item.SourceRow)
                           {
                               if (!Mapping.ColumnMapsToDestination(dcci))
                                   continue;

                               //Ignore NULL Values
                               if (dcci.BeforeColumnValue == null)
                                   continue;

                               string columnB = Mapping.MapColumnToDestination(dcci);
                               PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                               //Read-Only Column?
                               if (columnInfo.ReadOnly) continue;

                               if (columnInfo.IsRoot)
                               {
                                   if (columnInfo.IsMultiValue)
                                   {

                                   }
                                   else if (columnInfo.IsMultiIndexValue)
                                   {
                                       var related = PodioDataSchema.GetRelatedSchemaItems(columnInfo, DataSourceReader.PodioSchema.Columns);
                                       //Create an Array of Related Items
                                       itemObject[columnInfo.Name] = GetRelatedValues(related, item, columnInfo);
                                   }
                                   else
                                   {
                                       itemObject[columnInfo.Name] = dcci.BeforeColumnValue == null
                                                                         ? null
                                                                         : PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType, columnInfo.DataType);

                                   }
                               }
                           }

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
                               using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                               {
                                   dynamic result = Json.Decode(sr.ReadToEnd());

                                   Automation?.AfterAddItem(this, item, null);

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
                        #region Update Item

                        Automation?.BeforeUpdateItem(this, item, entityIDValue);

                        if (item.Sync)
                        {
                            //might need to call Podio for the contact record to merge the data.
                            dynamic contact = null;

                            var url = string.Format("https://api.podio.com/contact/{0}", entityIDValue);
                            if (DataSourceReader.Silent)
                                url += "?silent=1";

                            HttpWebRequest webRequest = WebRequest.CreateHttp(url);
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "PUT";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

                            var itemObject = new Dictionary<string, dynamic>();

                            foreach (DataCompareColumnItem dcci in item.Row)
                            {
                                if (!Mapping.ColumnMapsToDestination(dcci))
                                    continue;

                                string columnB = Mapping.MapColumnToDestination(dcci);
                                PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                                //Read-Only Column?
                                if (columnInfo.ReadOnly) continue;

                                //Do not include self reference
                                if (columnB == "profile_id")
                                    continue;

                                if (columnInfo.IsRoot)
                                {
                                    if (columnInfo.IsMultiValue)
                                    {

                                    }
                                    else if (columnInfo.IsMultiIndexValue)
                                    {
                                        //Get the Contact record so we can merge the results.
                                        if (contact == null)
                                            contact = DataSourceReader.GetContact(entityIDValue);

                                        var related = PodioDataSchema.GetRelatedSchemaItems(columnInfo, DataSourceReader.PodioSchema.Columns);
                                        //Create an Array of Related Items
                                        itemObject[columnInfo.Name] = MergeRelatedValues(related, item, columnInfo, contact);
                                    }
                                    else
                                    {
                                        itemObject[columnInfo.Name] = dcci.AfterColumnValue == null
                                                                          ? null
                                                                          : PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType);

                                    }
                                }
                            }

                            postData = Json.Encode(itemObject);

                            byte[] data = Encoding.UTF8.GetBytes(postData);

                            webRequest.ContentLength = data.Length;

                            using (Stream requestStream = webRequest.GetRequestStream())
                            {
                                requestStream.Write(data, 0, data.Length);
                            }

                            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                            {
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
                            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/contact/{0}", entityIDValue));
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "DELETE";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

                            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                            {
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
            DataSourceReader = reader as PodioContactsDataSourceReader;

            if (DataSourceReader != null)
            {
                Mapping = new DataSchemaMapping(SchemaMap, DataCompare);

                //Process the Changed Items
                if (addItems != null && status.ContinueProcessing) AddItems(addItems, status);
                if (updateItems != null && status.ContinueProcessing) UpdateItems(updateItems, status);
                if (deleteItems != null && status.ContinueProcessing) DeleteItems(deleteItems, status);

            }
        }

        

        private object[] GetRelatedValues(IList<string> related, DataCompareItemInvariant item, PodioDataSchemaItem columnInfo)
        {
            var values = new object[related.Count];
            
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = GetRelatedValue(related[i], item.SourceRow, columnInfo);                
            }

            return values;
        }
 
        private object GetRelatedValue(string related, IEnumerable<DataCompareColumnItem> items, PodioDataSchemaItem columnInfo)
        {
            foreach (var dcci in items)
            {
                if (!Mapping.ColumnMapsToDestination(dcci))
                    continue;

                if (Mapping.MapColumnToDestination(dcci) == related)
                {
                    return PodioDataSchemaTypeConverter.ConvertTo(dcci.BeforeColumnValue, columnInfo.PodioDataType, columnInfo.DataType);                    
                }
            }

            return null;
        }

        private object[] MergeRelatedValues(IList<string> related, DataCompareItemInvariant item, PodioDataSchemaItem columnInfo, dynamic contact)
        {
            var values = new object[related.Count];

            for (int i = 0; i < values.Length; i++)
            {
                //Get Original Value from source Json Object
                if (contact.ContainsKey(columnInfo.Name))
                {
                    if (contact[columnInfo.Name].Length > i)
                        values[i] = contact[columnInfo.Name][i];
                }

                //Locate Updated Value in Row Collection
                object val;
                if (MergeRelatedValue(related[i], item.Row, columnInfo, out val))
                    values[i] = val;
            }

            return values;
        }

        private bool MergeRelatedValue(string related, IEnumerable<DataCompareColumnItem> items, PodioDataSchemaItem columnInfo, out object result)
        {
            result = null;

            foreach (var dcci in items)
            {
                if (!Mapping.ColumnMapsToDestination(dcci))
                    continue;

                if (Mapping.MapColumnToDestination(dcci) == related)
                {
                    result = PodioDataSchemaTypeConverter.ConvertTo(dcci.AfterColumnValue, columnInfo.PodioDataType, columnInfo.DataType);
                    return true;
                }
            }

            return false;
        }
    }
}




