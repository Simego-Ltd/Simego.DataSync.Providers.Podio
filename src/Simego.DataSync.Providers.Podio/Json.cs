using System;
using System.Web.Script.Serialization;

namespace Simego.DataSync.Providers.Podio
{   
    public static class Json
    {
        public static dynamic Decode(string json)
        {
            var jss = new JavaScriptSerializer { MaxJsonLength = Int32.MaxValue, RecursionLimit = Int32.MaxValue };
            return jss.Deserialize<dynamic>(json);
        }

        public static string Encode(object value)
        {
            var jss = new JavaScriptSerializer { MaxJsonLength = Int32.MaxValue, RecursionLimit = Int32.MaxValue };
            return jss.Serialize(value);
        }
    }
}
