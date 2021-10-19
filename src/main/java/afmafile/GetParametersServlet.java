package afmafile;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/* Obtain user inputs and invoke the main processing routine.
 * 
 */

@WebServlet(value = "/")
public class GetParametersServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>AFMA File Processing</title></head><body>");
		out.println("<h3>Please enter required information</h3>");
		// out.println("<form action=\"startrun\" method=\"post\" autocomplete=\"off\">"
		out.println("<form action=\"startrun\" method=\"post\">"
				+ "<input type=\"text\" name=\"connectionString\" size=\"80\" placeholder=\"connectionString\">"
				+ "<input type=\"text\" name=\"blobPrefix\" size=\"80\" placeholder=\"blobPrefix\">"
				+ "<input type=\"text\" name=\"blobContainer\" size=\"80\" placeholder=\"blobContainer\">"
				+ "<p></p>"
				+ "<input type=\"submit\" value=\"Submit\">" + "</form>");
		out.println("</body></html>");
		out.close();
	}
}