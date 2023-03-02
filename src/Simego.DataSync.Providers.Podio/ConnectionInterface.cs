using System;
using System.Collections.Generic;
using System.Windows.Forms;

namespace Simego.DataSync.Providers.Podio
{
    public partial class ConnectionInterface : UserControl
    {
        public PropertyGrid PropertyGrid { get { return propertyGrid1; } }

        public ConnectionInterface()
        {
            InitializeComponent();
            Setup();
        }

        public void Setup()
        {
            PropertyGrid.LineColor = System.Drawing.Color.WhiteSmoke;
        }
    }
}
