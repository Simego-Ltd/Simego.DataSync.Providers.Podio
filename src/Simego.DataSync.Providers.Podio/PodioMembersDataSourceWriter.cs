using Simego.DataSync.Engine;
using Simego.DataSync.Interfaces;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Simego.DataSync.Providers.Podio
{
    public class PodioMembersDataSourceWriter : DataWriterProviderBase
    {
        private PodioMembersDataSourceReader DataSourceReader { get; set; }
        private DataSchemaMapping Mapping { get; set; }

        public override void AddItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items.Any())
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

                            var url = string.Format("https://api.podio.com/space/{0}/member/", DataSourceReader.SpaceID);

                            HttpWebRequest webRequest = WebRequest.CreateHttp(url);
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "POST";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

                            var itemObject = new Dictionary<string, dynamic>();

                            // Get the Default Role to apply
                            itemObject["role"] = DataSourceReader.Role.ToString().ToLowerInvariant();

                            if (!string.IsNullOrEmpty(DataSourceReader.Message))
                                itemObject["message"] = DataSourceReader.Message;

                            foreach (DataCompareColumnItem dcci in item.SourceRow)
                            {
                                if (!Mapping.ColumnMapsToDestination(dcci))
                                    continue;

                                string columnB = Mapping.MapColumnToDestination(dcci);
                                PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                                if (columnInfo.Name == "user_id")
                                {
                                    if (dcci.BeforeColumnValue != null)
                                    {
                                        itemObject["users"] = new[] { PodioDataSchemaTypeConverter.ConvertTo<int>(dcci.BeforeColumnValue, columnInfo.PodioDataType) };
                                    }
                                }
                                else if (columnInfo.Name == "mail")
                                {
                                    if (dcci.BeforeColumnValue != null)
                                    {
                                        itemObject["mails"] = new[] { PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.BeforeColumnValue, columnInfo.PodioDataType) };
                                    }
                                }
                                else if (columnInfo.Name == "role")
                                {
                                    if (dcci.BeforeColumnValue != null)
                                    {
                                        itemObject["role"] = PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.BeforeColumnValue, columnInfo.PodioDataType);
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
                                DataSourceReader.UpdateRateLimits(response, 2);

                                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                                {
                                    dynamic result = Json.Decode(sr.ReadToEnd());

                                    Automation?.AfterAddItem(this, item, PodioDataSchemaTypeConverter.ConvertToInvariant<long>(result[0]["profile"]["user_id"]));
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
            if (items.Any())
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

                            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/space/{0}/member/{1}", DataSourceReader.SpaceID, entityIDValue));
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "PUT";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

                            var itemObject = new Dictionary<string, dynamic>();

                            // Get the Default Role to apply
                            itemObject["role"] = DataSourceReader.Role.ToString().ToLowerInvariant();
                           
                            foreach (DataCompareColumnItem dcci in item.SourceRow)
                            {
                                if (!Mapping.ColumnMapsToDestination(dcci))
                                    continue;

                                string columnB = Mapping.MapColumnToDestination(dcci);
                                PodioDataSchemaItem columnInfo = DataSourceReader.PodioSchema.Columns[columnB];

                                if (columnInfo.Name == "role")
                                {
                                    if (dcci.AfterColumnValue != null)
                                    {
                                        itemObject["role"] = PodioDataSchemaTypeConverter.ConvertTo<string>(dcci.AfterColumnValue, columnInfo.PodioDataType);
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
            if (items.Any())
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
                            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/space/{0}/member/{1}", DataSourceReader.SpaceID, entityIDValue));
                            webRequest.UserAgent = PodioHelper.USER_AGENT;
                            webRequest.Method = "DELETE";
                            webRequest.ContentType = "application/json";
                            webRequest.Accept = "application/json";
                            webRequest.ServicePoint.Expect100Continue = false;
                            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", DataSourceReader.AccessToken));

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
            DataSourceReader = reader as PodioMembersDataSourceReader;

            if (DataSourceReader != null)
            {
                Mapping = new DataSchemaMapping(SchemaMap, DataCompare);

                //Process the Changed Items
                if (addItems != null && status.ContinueProcessing) AddItems(addItems, status);
                if (updateItems != null && status.ContinueProcessing) UpdateItems(updateItems, status);
                if (deleteItems != null && status.ContinueProcessing) DeleteItems(deleteItems, status);

            }
        }

    }
}



