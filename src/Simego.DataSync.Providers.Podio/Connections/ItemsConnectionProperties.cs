using System.Collections.Generic;
using Simego.DataSync.Providers.Podio.TypeConverters;
using Simego.DataSync.Providers.Podio.TypeEditors;
using System;
using System.ComponentModel;
using System.Drawing.Design;

namespace Simego.DataSync.Providers.Podio
{
    class ItemsConnectionProperties
    {
        private readonly PodioItemsDataSourceReader _reader;
        
        [Description("Podio Service Credentials")]
        [Category("Service")]
        [Editor(typeof(OAuthCredentialsWebTypeEditor), typeof(UITypeEditor))]
        public string Credentials { get { return _reader.Credentials; } set { _reader.Credentials = value; } }

        [Description("Podio Service Authentication Mode")]
        [Category("Service")]
        public PodioAuthenticationType AuthenticationMode { get { return _reader.AuthenticationMode; } set { _reader.AuthenticationMode = value; } }
    
        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        public string AccessToken { get { return _reader.AccessToken; } set { _reader.AccessToken = value; } }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        public string RefreshToken { get { return _reader.RefreshToken; } set { _reader.RefreshToken = value; } }

        [Category("Service")]
        [ReadOnly(true)]
        [Browsable(false)]
        public DateTime TokenExpires { get { return _reader.TokenExpires; } set { _reader.TokenExpires = value; } }

        [Category("Connection")]
        [TypeConverter(typeof (PodioAppTypeConverter))]
        [Description("App in Podio to connect")]
        public string App { get { return _reader.App; } set { _reader.App = value; } }

        [Category("Connection")]
        [Description("ID of App in Podio")]
        [ReadOnly(true)]
        [Browsable(false)]
        public int AppID { get { return _reader.AppID; } set { _reader.AppID = value; } }

        [Category("Connection")]
        [Description("Do not report changes to Podio Activity Stream")]
        public bool Silent { get { return _reader.Silent; } set { _reader.Silent = value; } }

        [Category("OAuth")]
        [Description("OAuth Client ID")]
        [ProviderCacheSetting(Name = "PodioDataSourceReader.ClientID")]
        public string ClientID { get { return _reader.ClientID; } set { _reader.ClientID = value; } }

        [Category("OAuth")]
        [Description("OAuth Client Secret")]
        [ProviderCacheSetting(Name = "PodioDataSourceReader.ClientSecret")]
        public string ClientSecret { get { return _reader.ClientSecret; } set { _reader.ClientSecret = value; } }


        public ItemsConnectionProperties(PodioItemsDataSourceReader reader)
        {
            _reader = reader;
        }

        public PodioItemsDataSourceReader GetReader()
        {
            return _reader;
        }

        internal string GetClientID()
        {            
            return _reader.GetClientID();
        }

        internal string GetClientSecret()
        {
            return _reader.GetClientSecret();
        }

        internal Dictionary<string, int> GetOrgApps()
        {
            return _reader.GetOrgApps();
        }
    }    
}
