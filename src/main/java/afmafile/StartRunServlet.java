package afmafile;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

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
			CancelRunServlet.resetPendingCancel();
			PrintWriter out = response.getWriter();
			out.println("<html><head><title>AFMA File Processing</title></head><body>");
			// then write the data of the response
			String connectionString = request.getParameter("connectionString");
			String blobPrefix = request.getParameter("blobPrefix");
			String blobContainer = request.getParameter("blobContainer");
			if (connectionString != null && connectionString.length() > 0 && blobPrefix != null
					&& blobPrefix.length() > 0 && blobContainer != null && blobContainer.length() > 0) {

				logger.info("Starting conversion run at " + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")).toString());
				runIsInProgress = true;
				MonitorRunServlet.clearUpdates();
				MonitorRunServlet.addUpdate("Starting conversion run");
				
				out.println("<h2>Now commencing processing of " + blobContainer + ".</h2>");
				out.println("<p/>");

				out.println("<form action=\"monitorrun\" method=\"get\">"
						+ "<input type=\"submit\" value=\"Click here to monitor run.\">" + "</form>");
				out.println("</body></html>");
				out.close();

				OverallConversionRun a2a = new OverallConversionRun(connectionString, blobPrefix, blobContainer);
				a2a.start();
			} else {
				out.println("<h2>All inputs must be provided. Start again and enter them all.</h2>");
				out.println("</body></html>");
				out.close();
			}
		} else {
			response.sendRedirect(request.getContextPath() + "/monitorrun");
		}
	}
}