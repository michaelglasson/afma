package specialConverter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Check the input paramaters and run the main processing routine.
 * 
 */

@WebServlet(value = "/startrun")
public class StartRunServlet extends HttpServlet {
	static final long serialVersionUID = 1L;
	static Logger logger = LoggerFactory.getLogger(StartRunServlet.class);
	static boolean runIsInProgress = false;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		if (!runIsInProgress) {
			// then write the data of the response
			String connectionString = request.getParameter("connectionString");
			String blobPrefix = request.getParameter("blobPrefix");
			String blobContainer = request.getParameter("blobContainer");
			String accessKey = request.getParameter("accessKey");
			
			if (connectionString != null && connectionString.length() > 0 && blobPrefix != null
					&& blobPrefix.length() > 0 && blobContainer != null && blobContainer.length() > 0
					&& accessKey != null && accessKey.length() > 0
					&& System.getenv("accessKey") != null
					&& accessKey.contentEquals(System.getenv("accessKey"))) {

				runIsInProgress = true; // Prevents restarting run during run
				CancelRunServlet.resetPendingCancel();
				MonitorRunServlet.clearUpdates(); // Updates are left in place until new run starts

				// The conversion runs in its own thread, allowing us to go to the monitoring
				// page while conversion runs
				OverallConversionRun a2a = new OverallConversionRun(connectionString, blobPrefix, blobContainer);
				a2a.start();
				response.sendRedirect(request.getContextPath() + "/monitorrun");

			} else {
				PrintWriter out = response.getWriter();
				out.println("<html><head><title>AFMA File Processing</title></head><body>");
				out.println("<h2>All inputs must be provided. Start again and enter them all.</h2>");
				out.println("</body></html>");
				out.close();
			}
		} else {
			response.sendRedirect(request.getContextPath() + "/monitorrun");
		}
	}
}