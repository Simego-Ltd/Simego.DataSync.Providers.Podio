using Simego.DataSync.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace Simego.DataSync.Providers.Podio
{
    internal static class PodioHelper
    {
        public static readonly string CLIENT_ID = "client-id"; //Default Client-ID
        public static readonly string CLIENT_SECRET = "client-secret"; //Default Client-Secret

        public static readonly string APP_GRANT = "app";

        public static readonly string USER_AGENT = string.Format("DataSync/{0} (compatible; DataSync {0}; {1};)", typeof(PodioHelper).Assembly.GetName().Version.ToString(3), Environment.OSVersion.VersionString);

        public static Dictionary<string, int> GetOrgApps(string accessToken)
        {
            var result = new Dictionary<string, int>();
            var orgs = GetOrgs(accessToken);
            
            foreach (var org in orgs.Keys)
            {
                var spaces = GetOrgSpaces(accessToken, orgs[org]);
                foreach (var space in spaces.Keys)
                {
                    var apps = GetOrgApps(accessToken, spaces[space]);
                    foreach (var app in apps.Keys)
                    {
                        var path = $"{org}/{space}/{app}";
                        result[path] = apps[app];
                    }
                }
            }
            
            return result;
        }

        public static Dictionary<string, int> GetOrgApps(string accessToken, int spaceid)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();

            HttpWebRequest appWebRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/app/space/{0}/", spaceid));
            appWebRequest.UserAgent = PodioHelper.USER_AGENT;
            appWebRequest.Method = "GET";
            appWebRequest.ServicePoint.Expect100Continue = false;
            appWebRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

            using (HttpWebResponse response = (HttpWebResponse)appWebRequest.GetResponse())
            {
                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic schema = Json.Decode(sr.ReadToEnd());
                    foreach (var app in schema)
                    {
                        int ix = 0;
                        var val = app["config"]["name"];
                        while (result.ContainsKey(val))
                            val = string.Format("{0}/{1}", app["config"]["name"], ++ix);

                        result.Add(val, Convert.ToInt32(app["app_id"]));
                    }
                }
            }

            return result;
        }

        public static string GetOrgSpaceName(string accessToken, int spaceid)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();

            HttpWebRequest appWebRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/space/{0}/", spaceid));
            appWebRequest.UserAgent = PodioHelper.USER_AGENT;
            appWebRequest.Method = "GET";
            appWebRequest.ServicePoint.Expect100Continue = false;
            appWebRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

            using (HttpWebResponse response = (HttpWebResponse)appWebRequest.GetResponse())
            {
                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic schema = Json.Decode(sr.ReadToEnd());
                    return schema["name"];
                }
            }            
        }

        public static Dictionary<string, int> GetOrgSpaces(string accessToken)
        {
            var result = new Dictionary<string, int>();
            var orgs = GetOrgs(accessToken);

            foreach (var org in orgs.Keys)
            {
                var spaces = GetOrgSpaces(accessToken, orgs[org]);
                foreach (var space in spaces.Keys)
                {
                    var path = $"{org}/{space}";
                    result[path] = spaces[space];
                }
            }

            return result;
        }

        public static Dictionary<string, int> GetOrgSpaces(string accessToken, int orgid)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();

            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/org/{0}/space/", orgid));
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
            {

                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic schema = Json.Decode(sr.ReadToEnd());

                    foreach (var space in schema)
                    {
                        int ix = 0;
                        var val = space["name"];
                        while (result.ContainsKey(val))
                            val = string.Format("{0}/{1}", space["name"], ++ix);

                        result.Add(val, Convert.ToInt32(space["space_id"]));
                    }
                }
            }

            return result;
        }
        public static Dictionary<string, int> GetOrgs(string accessToken)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();

            HttpWebRequest webRequest = WebRequest.CreateHttp("https://api.podio.com/org/");
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
            {

                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic schema = Json.Decode(sr.ReadToEnd());

                    foreach (var org in schema)
                    {
                        result[org["name"]] = Convert.ToInt32(org["org_id"]);
                    }
                }
            }

            return result;
        }

        public static Dictionary<string, int> GetAppViews(string accessToken, int appID)
        {
            Dictionary<string, int> result = new Dictionary<string, int>();

            //Add Default All Items View
            result.Add("All Items", 0);

            HttpWebRequest webRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/view/app/{0}/", appID));
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

            using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
            {
                using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                {
                    dynamic schema = Json.Decode(sr.ReadToEnd());

                    foreach (var view in schema)
                    {
                        int ix = 0;
                        var val = view["name"];
                        while (result.ContainsKey(val))
                            val = string.Format("{0}/{1}", view["name"], ++ix);                        
                        
                        result.Add(val, view["view_id"]);
                    }
                }
            }

            return result;
        }

        public static int GetOrgAppIdFromPath(string accessToken, int spaceid, string path)
        {
            var sp = new StringSplit(path, '|');
            if (!string.IsNullOrEmpty(sp.Value2))
            {
                return PodioDataSchemaTypeConverter.ConvertToInvariant<int>(sp.Value2);
            }

            var parts = sp.Value1.Split('/');
            // Length == 1 therefore lookup App in Current Space.
            if (parts.Length == 1)
            {
                if (spaceid == 0) throw new ArgumentOutOfRangeException(nameof(spaceid), $"Podio SpaceID cannot be 0.");
                
                HttpWebRequest appWebRequest = WebRequest.CreateHttp(string.Format("https://api.podio.com/app/space/{0}/", spaceid));
                appWebRequest.UserAgent = PodioHelper.USER_AGENT;
                appWebRequest.Method = "GET";
                appWebRequest.ServicePoint.Expect100Continue = false;
                appWebRequest.Headers.Add("Authorization", string.Format("OAuth2 {0}", accessToken));

                using (HttpWebResponse response = (HttpWebResponse)appWebRequest.GetResponse())
                {
                    using (StreamReader sr = new StreamReader(response.GetResponseStream()))
                    {
                        dynamic schema = Json.Decode(sr.ReadToEnd());
                        foreach (var app in schema)
                        {
                            if(string.Equals(app["config"]["name"], parts[0], StringComparison.OrdinalIgnoreCase))
                            {
                                return PodioDataSchemaTypeConverter.ConvertToInvariant<int>(app["app_id"]);
                            }
                        }
                    }
                }
            }
            else if(parts.Length >= 3)
            {
                var orgName = parts[0];
                var spaceName = parts[1];
                if(parts.Length > 3)
                {
                    spaceName = $"{spaceName}/{parts[2]}";
                }                
                var appName = parts[parts.Length - 1];

                //get orgs
                if(GetOrgs(accessToken).TryGetValue(orgName, out int orgId))
                {
                    //get spaces
                    if(GetOrgSpaces(accessToken, orgId).TryGetValue(spaceName, out int spaceId))
                    {
                        //get apps
                        if(GetOrgApps(accessToken, spaceId).TryGetValue(appName, out int appId))
                        {
                            return appId;
                        }
                    }
                }                               
            }
          
            return 0;
        }

        public static void HandleError(IDataSynchronizationStatus status, string postData, WebException e)
        {           
            if (status.FailOnError)
            {
                throw new DataSynchronisationWriterException<string>(e.Message, postData, e);
            }

            status.LogMessage(e.Message);
        }

    }
}
