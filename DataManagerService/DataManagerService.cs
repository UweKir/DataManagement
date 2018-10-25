using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using DataManager;

namespace DataManagerService
{
    public partial class DataManagerService : ServiceBase
    {
        
        private static DataManager.Receiver receiver = null;


        public DataManagerService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            startAgent();
        }

        protected override void OnStop()
        {
            
        }

        public void startAgent()
        {
            if (receiver == null)
            {
                receiver = new DataManager.Receiver(Properties.Settings.Default.ConfigPath);

                if (receiver.init())
                    receiver.start();

            }

            receiver.start();
        }
    }
}
