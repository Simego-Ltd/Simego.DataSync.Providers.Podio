using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace Simego.DataSync.Providers.Podio
{
    public class Podio
    {
        private readonly Action<HttpWebRequest> _authenticateRequestCallback;

        internal Podio(Action<HttpWebRequest> authenticateRequest)
        {
            _authenticateRequestCallback = authenticateRequest;
        }

        public dynamic JsonRequest(Uri uri, string method = "GET")
        {
            return JsonRequest(uri, method, null);
        }

        public dynamic JsonRequest(Uri uri, string method, object body)
        {
            var request = WebRequest.CreateHttp(uri);
            request.UserAgent = PodioHelper.USER_AGENT;
            request.Method = method;

            return JsonRequest(request, Json.Encode(body));
        }

        public dynamic JsonRequest(Uri uri, string method, string body)
        {
            var request = WebRequest.CreateHttp(uri);
            request.UserAgent = PodioHelper.USER_AGENT;
            request.Method = method;
            
            return JsonRequest(request, body);
        }

        public dynamic JsonRequest(HttpWebRequest request)
        {
            return JsonRequest(request, null);
        }

        public dynamic JsonRequest(HttpWebRequest request, string body)
        {
            _authenticateRequestCallback(request);
            request.ContentType = "application/json";
            request.Accept = "application/json";

            if (!string.IsNullOrEmpty(body))
            {
                byte[] data = Encoding.UTF8.GetBytes(body);

                request.ContentLength = data.Length;

                using (Stream requestStream = request.GetRequestStream())
                {
                    requestStream.Write(data, 0, data.Length);
                }
            }

            using (var response = (HttpWebResponse)request.GetResponse())
            {                
                using (var sr = new StreamReader(response.GetResponseStream()))
                {
                    return Json.Decode(sr.ReadToEnd());
                }
            }
        }

        /// <summary>
        /// Add a Status message to the Space Activiy Stream.
        /// </summary>
        /// <param name="spaceID">ID of the Space in Podio</param>
        /// <param name="message">Message to add</param>
        public void AddStatusMessage(int spaceID, string message)
        {            
            JsonRequest(new Uri(string.Format("https://api.podio.com/status/space/{0}/", spaceID)), "POST", new { value = message });
        }

        /// <summary>
        /// Download a File from Podio.
        /// </summary>
        /// <param name="url">URL of File in Podio</param>
        /// <returns>File data as a Byte[]</returns>
        public byte[] GetFile(string url)
        {
            var webRequest = WebRequest.CreateHttp(url);
            webRequest.UserAgent = PodioHelper.USER_AGENT;
            webRequest.Method = "GET";
            
            _authenticateRequestCallback(webRequest);

            using (var response = (HttpWebResponse) webRequest.GetResponse())
            {
                using (var m = new MemoryStream())
                {
                    var buffer = new byte[16384];

                    using (var sr = response.GetResponseStream())
                    {
                        int bytesRead;
                        while (sr != null && (bytesRead = sr.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            m.Write(buffer, 0, bytesRead);
                        }
                    }

                    m.Flush();
                    
                    return m.ToArray();
                }
            }
        }

        public Dictionary<string, int> GetOrgSpaces()
        {
            var result = new Dictionary<string, int>();

            dynamic schema = JsonRequest(new Uri("https://api.podio.com/org/"));

            foreach (var org in schema)
            {
                foreach (var space in org["spaces"])
                {
                    int ix = 0;
                    var val = string.Format("{0}/{1}", org["name"], space["name"]);
                    while (result.ContainsKey(val))
                        val = string.Format("{0}/{1}", string.Format("{0}/{1}", org["name"], space["name"]), ++ix);

                    result.Add(val, Convert.ToInt32(space["space_id"]));                    
                }
            }

            return result;
        }
    }
}
