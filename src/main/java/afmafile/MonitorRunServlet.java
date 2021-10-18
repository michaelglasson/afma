package afmafile;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/* This class holds an array of progress strings and displays them.
 * Progress updates can be found by pressing the browser refresh
 * button. The main processing loop consults the value of toCancel
 * and if it is true, returns on completion of the current file.
 */

@WebServlet(value = "/monitorrun")
public class MonitorRunServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	// The updates array holds progess report information
	private static final List<String> updates = new ArrayList<>();
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
	    response.setIntHeader("Refresh", 5);
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>AFMA File Processing</title></head><body>");
		out.println("<h3>Progress is updated every 5 seconds, or you can click the reload button on your browser.</h3>");
		out.println("<form action=\"cancelrun\" method=\"get\">"
				+ "<input type=\"submit\" value=\"Click here to CANCEL run after current file is processed.\">"
				+ "</form>");
		out.println("<h3>Run Progress Report</h3>");
		for (String s : updates) {
			out.print(s + "<br>");
		}
		out.println("</body></html>");
		out.close();
	}

	public static void clearUpdates() {
		updates.clear();
	}

	public static void addUpdate(String updateString) {
		updates.add(updateString);
	}
}