using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PluginInterface
{
    public class DriverAddressIoArgModel
    {
        public Guid ID { get; set; }

        private string _address;
        public string Address { get {
                return _address.Replace("&quot;", "\"");//双引号的处理
            } set {
                _address = value;
            } }
        public object Value { get; set; }
        public DataTypeEnum ValueType { get; set; }
        public override string ToString()
        {
            return $"变量ID:{ID},Address:{Address},Value:{Value},ValueType:{ValueType}";
        }
    }
}
