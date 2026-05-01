# =========================================================
# fun_lmrd_preprocess.R
# Fast point-in-polygon utilities for LMRD features
# =========================================================
#
# This file defines a Rcpp implementation of a point-in-polygon
# test used to compute L-moment Ratio Diagram (LMRD) features.
#
# The function point_in_polygon() expects a preprocessed polygon
# object, distributed in:
#
#   data/input_data/poligonos_pre.rds
#
# The object must contain:
#   - polygons: a named list of preprocessed polygon boundaries;
#   - tol: numerical tolerance.
# =========================================================

library(Rcpp)


# ---------------------------------------------------------
# C++ point-in-polygon function
# ---------------------------------------------------------

cppFunction('
bool point_in_polygon_cpp(double px,
                          double py,
                          NumericVector x1,
                          NumericVector y1,
                          NumericVector x2,
                          NumericVector y2,
                          NumericVector dx,
                          NumericVector dy,
                          NumericVector bbox,
                          double tol) {

  int n = x1.size();

  if (px < bbox[0] - tol || px > bbox[1] + tol ||
      py < bbox[2] - tol || py > bbox[3] + tol) {
    return false;
  }

  for (int i = 0; i < n; i++) {
    double cx = (px - x1[i]) * (y2[i] - y1[i]) - (py - y1[i]) * (x2[i] - x1[i]);

    if (std::abs(cx) <= tol) {
      double xmin = std::min(x1[i], x2[i]);
      double xmax = std::max(x1[i], x2[i]);
      double ymin = std::min(y1[i], y2[i]);
      double ymax = std::max(y1[i], y2[i]);

      if (px >= xmin - tol && px <= xmax + tol &&
          py >= ymin - tol && py <= ymax + tol) {
        return true;
      }
    }
  }

  int count = 0;

  for (int i = 0; i < n; i++) {
    bool cond = (y1[i] > py) != (y2[i] > py);

    if (cond) {
      double xint = x1[i] + (py - y1[i]) * dx[i] / dy[i];
      if (xint > px) {
        count++;
      }
    }
  }

  return (count % 2) == 1;
}
')


# ---------------------------------------------------------
# R wrapper for point-in-polygon test
# ---------------------------------------------------------

point_in_polygon <- function(px, py, boundary_id, objeto_pre) {
  
  if (is.null(objeto_pre) || !is.list(objeto_pre)) {
    stop("objeto_pre must be a preprocessed polygon object.")
  }
  
  if (!"polygons" %in% names(objeto_pre)) {
    stop("objeto_pre must contain a 'polygons' element.")
  }
  
  if (!"tol" %in% names(objeto_pre)) {
    stop("objeto_pre must contain a 'tol' element.")
  }
  
  if (length(px) != 1 || length(py) != 1) {
    stop("px and py must be scalar numeric values.")
  }
  
  if (!is.finite(px) || !is.finite(py)) {
    return(FALSE)
  }
  
  boundary_id <- as.character(boundary_id)
  
  poly <- objeto_pre$polygons[[boundary_id]]
  
  if (is.null(poly)) {
    stop("boundary_id not found in preprocessed polygon object: ", boundary_id)
  }
  
  required_poly_fields <- c("x1", "y1", "x2", "y2", "dx", "dy", "bbox")
  
  missing_fields <- setdiff(required_poly_fields, names(poly))
  
  if (length(missing_fields) > 0) {
    stop(
      "Polygon object for boundary_id '",
      boundary_id,
      "' is missing fields: ",
      paste(missing_fields, collapse = ", ")
    )
  }
  
  point_in_polygon_cpp(
    px = px,
    py = py,
    x1 = poly$x1,
    y1 = poly$y1,
    x2 = poly$x2,
    y2 = poly$y2,
    dx = poly$dx,
    dy = poly$dy,
    bbox = unname(poly$bbox),
    tol = objeto_pre$tol
  )
}