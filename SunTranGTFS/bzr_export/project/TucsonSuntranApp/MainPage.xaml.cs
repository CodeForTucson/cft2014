using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;
using Windows.UI.Xaml.Controls.Maps;
using Windows.Devices.Geolocation;

// The Blank Page item template is documented at http://go.microsoft.com/fwlink/?LinkId=391641

namespace TucsonSuntranApp
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class MainPage : Page
    {
        public MainPage()
        {
            this.InitializeComponent();

            this.NavigationCacheMode = NavigationCacheMode.Required;
        }

        /// <summary>
        /// Invoked when this page is about to be displayed in a Frame.
        /// </summary>
        /// <param name="e">Event data that describes how this page was reached.
        /// This parameter is typically used to configure the page.</param>
        protected override void OnNavigatedTo(NavigationEventArgs e)
        {
            // TODO: Prepare page for display here.

            // TODO: If your application contains multiple pages, ensure that you are
            // handling the hardware Back button by registering for the
            // Windows.Phone.UI.Input.HardwareButtons.BackPressed event.
            // If you are using the NavigationHelper provided by some templates,
            // this event is handled for you.

            Debug.WriteLine("heygirl");


            // make a web request
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create("http://mgrandi.no-ip.org:81/sunspot");

            request.BeginGetResponse(GetDataCallback, request);
        }

        /// <summary>
        /// callback for when we ask for the data from my server
        /// </summary>
        /// <param name="result"></param>
        async void GetDataCallback(IAsyncResult result)
        {

            List<BasicGeoposition> listOfGeoPoints = new List<BasicGeoposition>();
            GTFSData.Types.TempListOfStops stops = null;

            HttpWebRequest request = result.AsyncState as HttpWebRequest;
            if (request != null)
            {
                try
                {
                    WebResponse response = request.EndGetResponse(result);




                    stops = GTFSData.Types.TempListOfStops.ParseFrom(response.GetResponseStream());

                    
                }
                catch (WebException e)
                {
                    Debug.WriteLine(e);
                    return;
                }

            } // end if 

            foreach (GTFSData.Types.Stop iterStop in stops.StopsList)
            {

                Debug.WriteLine(iterStop.StopId);
                BasicGeoposition tmp = new BasicGeoposition();
                tmp.Altitude = 0;
                tmp.Latitude = double.Parse(iterStop.StopLat);
                tmp.Longitude = double.Parse(iterStop.StopLon);

                listOfGeoPoints.Add(tmp);

            }

            await theMapControl.Dispatcher.RunAsync(Windows.UI.Core.CoreDispatcherPriority.Normal, () =>
            {


                foreach (BasicGeoposition tmp in listOfGeoPoints)
                {

                    MapIcon icon = new MapIcon();
                    icon.Location = new Geopoint(tmp);
                    theMapControl.MapElements.Add(icon);


                }
                //MapPolyline pline = new MapPolyline();
                //pline.Path = new Geopath(listOfGeoPoints);

            });


        }

        private void mapZoomOutBtnClick(object sender, RoutedEventArgs e)
        {

            theMapControl.ZoomLevel -= 0.10;

        }

        private void mapZoomInBtnClick(object sender, RoutedEventArgs e)
        {

            theMapControl.ZoomLevel += 0.10;

        }
    }
}
