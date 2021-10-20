package specialConverter;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/* Display a page acknowledging receipt of the cancellation request.
 * Set cancelIsPending true so that the main processing loop will know that
 * it needs to exit.
 */

@WebServlet(value = "/cancelrun")
public class CancelRunServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	// Need to make sure this is reset at the start of a run.
	private static Boolean cancelIsPending = false;

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		cancelIsPending = true;
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>AFMA File Processing</title></head><body>");
		out.println("<h3>Run Cancelled. Processing will stop after the current file has been processed.</h3>");
		out.println("<h3>If the current file is large, cancellation may take some time.</h3>");
		out.println("<h3><a href=\"monitorrun\">Return to monitoring page</a></h3>");
		out.println("</body></html>");
		out.close();
		
		MonitorRunServlet.addUpdate("Cancellation of run requested by user. Run will finish when current file is processed.");
	}

	public static Boolean cancelIsPending() {
		return cancelIsPending;
	}
	
	public static void resetPendingCancel() {
		cancelIsPending = false;
	}
}