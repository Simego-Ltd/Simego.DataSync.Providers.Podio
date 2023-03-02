using System;
using System.Globalization;

namespace Simego.DataSync.Providers.Podio
{
    class PodioDataSchemaTypeConverter
    {       
        public static T ConvertToInvariant<T>(object value)
        {
            return DataSchemaTypeConverter.ConvertTo<T>(value, CultureInfo.InvariantCulture);
        }

        public static T ConvertTo<T>(object value, PodioDataSchemaItemType podioType)
        {
            return (T)ConvertTo(value, podioType, typeof(T));
        }

        public static object ConvertTo(object value, PodioDataSchemaItemType podioType, Type type)
        {
            switch (podioType)
            {
                case PodioDataSchemaItemType.Number:
                case PodioDataSchemaItemType.Money:
                    {
                        return DataSchemaTypeConverter.ConvertTo(value, type, CultureInfo.InvariantCulture);
                    }
                default:
                    {
                        var result = DataSchemaTypeConverter.ConvertTo(value, type);
                        if (type == typeof(string))
                        {
                            return TidyValue((string)result);
                        }

                        return result;
                    }
            }            
        }        

        private static string TidyValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return null;

            if (value.Equals("<p></p>", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
            if (value.StartsWith("<p>", StringComparison.OrdinalIgnoreCase))
            {
                return TidyValue(value.Substring(3));
            }                        
            if (value.EndsWith("</p>", StringComparison.OrdinalIgnoreCase))
            {
                return TidyValue(value.Substring(0, value.Length-4));
            }
            
            return value;
        }
    }
}
