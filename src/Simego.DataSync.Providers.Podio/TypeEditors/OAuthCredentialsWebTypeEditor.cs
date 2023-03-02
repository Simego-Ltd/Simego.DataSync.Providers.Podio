using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Drawing.Design;
using System.Net;
using System.Reflection;

namespace Simego.DataSync.Providers.Podio.TypeEditors
{
    class OAuthCredentialsWebTypeEditor : UITypeEditor, IDisposable
    {
        private const BindingFlags DefaultPropertyBinding = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty;
        
        private static readonly Random Random = new Random(Environment.TickCount);
        private readonly WebServer Server;
        private readonly int Port;

        private string _CallbackUrl => $"http://localhost:{Port}/authorise";
        private OAuthWebConnection _ConnectionDialog;
        private string _ClientID;
        private string _ClientSecret;
        private string _AccessToken;
        private string _RefreshToken;
        private DateTime _TokenExpires;
        
        public OAuthCredentialsWebTypeEditor()
        {
            Port = Random.Next(40000, 60000);

            Server = new WebServer(Port)
            {
                PageRequestCallback = ProcessRequest
            };
        }

        private WebMessage ProcessRequest(HttpListenerContext context)
        {
            var code = context.Request.QueryString["code"];
            if (!string.IsNullOrEmpty(code))
            {
                ValidateToken(code);
                if (!string.IsNullOrEmpty(_AccessToken) && !string.IsNullOrEmpty(_RefreshToken))
                {
                    //Close the Connection Dialog on the UI thread!
                    _ConnectionDialog.Invoke(new Action(_ConnectionDialog.Close));
                    context.Response.Redirect("https://podio.com");
                    return null;
                }                           
            }
            
            return new WebMessage { StatusCode = 200, Message = "NOT_OK", ContentType = "text/html" };
        }

        protected void ValidateToken(string code)
        {
            var helper = new HttpWebRequestHelper();
            var parameters = new Dictionary<string, string>
            {
                ["grant_type"] = "authorization_code",
                ["client_id"] = _ClientID,
                ["client_secret"] = _ClientSecret,
                ["code"] = code,
                ["redirect_uri"] = _CallbackUrl
            };

            var tokenResult = helper.PostRequestAsString(parameters, HttpWebRequestHelper.MimeTypeApplicationWwwFormUrlEncoded, "https://podio.com/oauth/token");

            dynamic result = Json.Decode(tokenResult);

            _AccessToken = result["access_token"];
            _RefreshToken = result["refresh_token"];
            _TokenExpires = DateTime.Now.AddSeconds(Convert.ToInt32(result["expires_in"]));
        }

        public override UITypeEditorEditStyle GetEditStyle(ITypeDescriptorContext context)
        {
            return UITypeEditorEditStyle.Modal;
        }

        public override object EditValue(ITypeDescriptorContext context, IServiceProvider provider, object value)
        {
            _ClientID = (string)context.Instance.GetType().InvokeMember("GetClientID", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, context.Instance, null);
            _ClientSecret = (string)context.Instance.GetType().InvokeMember("GetClientSecret", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, null, context.Instance, null);

            using (_ConnectionDialog = new OAuthWebConnection())
            {               
                var authoriseUrl = $"https://podio.com/oauth/authorize?response_type=code&client_id={Uri.EscapeDataString(_ClientID)}&redirect_uri={Uri.EscapeDataString(_CallbackUrl)}";

                //Open External Browser
                Process.Start(authoriseUrl);

                //Wait for the OAuth dance to be over....
                _ConnectionDialog.ShowDialog();

                PropertyInfo fAccessToken = context.Instance.GetType().GetProperty("AccessToken", DefaultPropertyBinding);
                PropertyInfo fRefreshToken = context.Instance.GetType().GetProperty("RefreshToken", DefaultPropertyBinding);
                PropertyInfo fTokenExpires = context.Instance.GetType().GetProperty("TokenExpires", DefaultPropertyBinding);

                fAccessToken.SetValue(context.Instance, _AccessToken, null);
                fRefreshToken.SetValue(context.Instance, _RefreshToken, null);
                fTokenExpires.SetValue(context.Instance, _TokenExpires, null);
            }

            return !string.IsNullOrEmpty(_AccessToken) && !string.IsNullOrEmpty(_RefreshToken) ? "Connected" : string.Empty;
        }

        private bool disposed = false;

        //Implement IDisposable.
        public void Dispose()
        {
            Dispose(true);            
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    // Free other state (managed objects).
                    Server?.Stop();                    
                }

                disposed = true;
            }
        }
    }
}
