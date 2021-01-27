#!/opt/PANGmisc/bin/python
# -*- coding: koi8-r -*-
# $Id: lines_geom.py 3636 2006-03-15 15:19:26Z efremov $
#
# Module used for optimization (reducing number of points)
# of seismic lines and for calculating crosses between
# polylines (partially taken from CrossCalculator.py utility).
#
# Points of entry (exported finctions):
#   opt_geom(inp_polyline, cdp_step) = optGeom2D(inp_polyline, cdp_step)
#   opt_geom3(inp_ref, d) = optGeom3D(inp_polyline, d)
#   crossLines(polyline1, polyline2)
#   nearestPoint(polyline, point)

__version__ = '$Revision: 3636 $'[11:-2]

import math

def distance (ref1, ref2):
    """Computes 2D cartesian distance between points referenced by
    1-st and 2-nd arguments"""
    return (math.sqrt( (ref1[0] - ref2[0])**2 + (ref1[1] - ref2[1])**2 ) )

def distance3 (ref1, ref2):
    """Computes 3D cartesian distance between points referenced by
    1-st and 2-nd arguments"""
    return (math.sqrt( (ref1[0] - ref2[0])**2 + (ref1[1] - ref2[1])**2 + (ref1[2] - ref2[2])**2 ) )

def distPt2Line(pts, pte, pt):
    """Calculates distance from point to line, defined by two points
    pts and pte on plane.
    """
    (p1_ref, p2_ref, lp_ref) = (pts, pte, pt)
    d = distance(p1_ref, p2_ref)
    if d == 0.0:
        raise ValueError("Input points are too close")

    # parameter corresponding to minimum distance
    # between point lp_ref and line connectiong p1_ref and p2_ref
    t = ( (lp_ref[0] - p1_ref[0]) * (p2_ref[0] - p1_ref[0]) + \
          (lp_ref[1] - p1_ref[1]) * (p2_ref[1] - p1_ref[1]) ) / (d * d)

    min_point = [p1_ref[0] + t * (p2_ref[0] - p1_ref[0]),
                 p1_ref[1] + t * (p2_ref[1] - p1_ref[1]),  0];
    sol_dist =  distance(min_point, lp_ref);
    return sol_dist


def distPt3Line(pts, pte, pt):
    """Calculates distance from point to line, defined by two points
    pts and pte in 3D space.
    """
    (p1_ref, p2_ref, lp_ref) = (pts, pte, pt)
    d = distance3(p1_ref, p2_ref)
    if d == 0.0:
        raise ValueError("Input points are too close")

    # parameter corresponding to minimum distance
    # between point lp_ref and line connectiong p1_ref and p2_ref
    t = ( (lp_ref[0] - p1_ref[0]) * (p2_ref[0] - p1_ref[0]) + 
          (lp_ref[1] - p1_ref[1]) * (p2_ref[1] - p1_ref[1]) +
          (lp_ref[2] - p1_ref[2]) * (p2_ref[2] - p1_ref[2]) ) / (d * d)

    min_point = [p1_ref[0] + t * (p2_ref[0] - p1_ref[0]),
                 p1_ref[1] + t * (p2_ref[1] - p1_ref[1]),
                 p1_ref[2] + t * (p2_ref[2] - p1_ref[2])];
    sol_dist =  distance3(min_point, lp_ref);
    return sol_dist

def sameline (lp_ref, outp_ref, dd_nom):
    """Verifies whether first point is on the same line as two last points
    of second argument (ref to array of points)
    3-d argument is the distance betwee succesive points in original
    array. Returns 1 (same line) when points are the same."""

    lst_idx = len(outp_ref) - 1

    (p1_ref, p2_ref) = (outp_ref[lst_idx - 1], outp_ref[lst_idx])
    (n1, n2) = (p1_ref[2], p2_ref[2])
    d = distance(p1_ref, p2_ref)
    if d == 0.0:
        return 1

    dd = distance(p2_ref, lp_ref)
    if dd < dd_nom * 0.001:
        # Identical points are (by definition) on the same line.
        return 1

    # parameter corresponding to minimum distance
    # between point lp_ref and line connectiong p1_ref and p2_ref
    t = ( (lp_ref[0] - p1_ref[0]) * (p2_ref[0] - p1_ref[0]) + \
          (lp_ref[1] - p1_ref[1]) * (p2_ref[1] - p1_ref[1]) ) / (d * d)

    min_point = [p1_ref[0] + t * (p2_ref[0] - p1_ref[0]),
                 p1_ref[1] + t * (p2_ref[1] - p1_ref[1]),  0];
    sol_dist =  distance(min_point, lp_ref);

    if sol_dist < dd_nom * 0.1:
        return 1
    else:
        return 0

def sameline3 (lp_ref, outp_ref, dd_nom):
    """Verifies whether first point is on the same line as two last points
    of second argument (ref to array of points)
    3-d argument is the distance between succesive points in original
    array. Returns 1 (same line) when points are the same. Points are in 3D space."""

    lst_idx = len(outp_ref) - 1

    (p1_ref, p2_ref) = (outp_ref[lst_idx - 1], outp_ref[lst_idx])
    d = distance3(p1_ref, p2_ref)
    if d == 0.0:
        return 1

    dd = distance3(p2_ref, lp_ref)
    if dd < dd_nom * 0.001:
        # Identical points are (by definition) on the same line.
        return 1

    # parameter corresponding to minimum distance
    # between point lp_ref and line connectiong p1_ref and p2_ref
    t = ( (lp_ref[0] - p1_ref[0]) * (p2_ref[0] - p1_ref[0]) + 
          (lp_ref[1] - p1_ref[1]) * (p2_ref[1] - p1_ref[1]) +
          (lp_ref[2] - p1_ref[2]) * (p2_ref[2] - p1_ref[2]) ) / (d * d)

    min_point = [p1_ref[0] + t * (p2_ref[0] - p1_ref[0]),
                 p1_ref[1] + t * (p2_ref[1] - p1_ref[1]),
                 p1_ref[2] + t * (p2_ref[2] - p1_ref[2])];
    sol_dist =  distance3(min_point, lp_ref);

    if sol_dist < dd_nom * 0.1:
        return 1
    else:
        return 0


def opt_geom(inp_ref, d):
    """Function to find piece-linear approximation for profiles geometry.
    Input:
        inp_ref - array of points [(x, y, cdp), ...]
        d       - CDP step (nominal distance between CDPs)
    Output:
        Array with optimised coordinates (same structure as inp_ref)"""
    return optGeom2D(inp_ref, d)

def opt_geom3(inp_ref, d = 0):
    """Function to find piece-linear approximation for geometry of line in 3D space.
    Input:
        inp_ref - array of points [(x, y, z), ...]
        d       - nominal distance petween successinve points
    Output:
        Array with optimised coordinates (same structure as inp_ref)"""
    # calculate mean distance between points if needed
    if d == 0:
        s = 0
        for i in range(len(inp_ref) - 1):
            s += distance3(inp_ref[i], inp_ref[i+1])
        d = s / len(inp_ref)
        print('DEBUG: mean dist', d)
    return optGeom3D(inp_ref, d)


def optGeomGeneric(inp_ln, accur, dist2line):
    """Here dist2line is a function to compute the distance
    from point to line in 2d or 3d space.
    """
    # find point with max. dist
    d = 0.
    ind = -1
    for i in range(1, len(inp_ln) - 1):
        pt = inp_ln[i]
        dn = dist2line(inp_ln[0], inp_ln[-1], pt)
        if dn > d:
            d = dn
            ind = i
##    print 'DEBUG: max point', ind, inp_ln[ind], d
    if d < accur:
        return [inp_ln[0], inp_ln[-1]]
    
    # split by i-th index
    ll_in = inp_ln[:ind+1]
    lr_in = inp_ln[ind:]
    # now, apply algorithm to both parts recursively:
    if len(ll_in) ==2:
        ll = ll_in # end of recursion
    else:
        ll = optGeomGeneric(ll_in, accur, dist2line)
    if len(lr_in) == 2:
        lr = lr_in
    else:
        lr = optGeomGeneric(lr_in, accur, dist2line)
    ans = ll + lr[1:]
    return ans

def optGeom2D(inp_ln, accur):
    """
    Return line with optimum geometry in 2D
    """
    return optGeomGeneric(inp_ln, accur, distPt2Line)

def optGeom3D(inp_ln, accur):
    """
    Return line with optimum geometry in 3D
    """
    return optGeomGeneric(inp_ln, accur, distPt3Line)


###########################################################
#  Calculating crosses

def calc_prof_crosses(self):
    "Computing intersections for one profile"
    profiles = list(self.dict.keys())
    q = float(len(profiles) ** 2 - len(profiles))
    cur = 0
    cur_proc = -1
    for p1 in profiles:
        for p2 in profiles:
            if p1 != p2:
                self.CrossProfiles(p1, p2)
                cur += 1
                proc = int((float(cur) / q) * 100.)
                if proc != cur_proc:
                    sys.stderr.write(str(proc) + '\n')
                    cur_proc = proc

def crossLines(pr1_points, pr2_points):
    """Calculates cross ponts for polylines pr1 and pr2
    Input:
      pr1_points, pr2_points: polylines - array of points [(x1, y1), (x2, y2), ...]
    Output:
      list of coordinates of crosses: [(crx1, cry1), (crx2, cry2), ...]
    """
    ans = []
    for i in range(0, len(pr1_points) - 1):
        pt1 = pr1_points[i]
        pt2 = pr1_points[i+1]
        for j in range(0, len(pr2_points) - 1):
            pt3 = pr2_points[j]
            pt4 = pr2_points[j+1]
            if IsSegmsIntersect(pt1, pt2, pt3, pt4):
                intersect = GetSegmsIntersection(pt1, pt2, pt3, pt4)
                ans.append(intersect)
    return ans



def GetSignOfPoint(start, end, point):
    """Calculates if point is placed to the left or right part
    of space relative to the line passing through the points start, end
    Input:
      start, end, point - coordinates of points (tuples)
    Output:
      0, 1, -1
    """
    d = (end[1] - start[1])*point[0] + \
        (start[0] - end[0])*point[1] - \
        start[0]*end[1] + end[0]*start[1]

    if d > 0.0:
        return 1
    elif d < 0.0:
        return -1
    else:
        return 0

def GetSegmsIntersection(start1, end1, start2, end2):
    """Calculates point of intersection for two lines
    passing through points (start1, end1) and (start2, end2)
    Input:
      start1, end1, start2, end2 - coordinates of points in (x, y)
         format
    Output:
      Tuple (crx, cry) with coordinates of intersection
    """
    a1 = end1[1] - start1[1]
    b1 = start1[0] - end1[0]
    c1 = -start1[0]*end1[1] + end1[0]*start1[1]
    a2 = end2[1] - start2[1]
    b2 = start2[0] - end2[0]
    c2 = -start2[0]*end2[1] + end2[0]*start2[1]
    res = ((b1*c2 - c1*b2) / (a1*b2 - a2*b1),
           (-a1*c2 + a2*c1) / (a1*b2 - a2*b1))
    return res


def round(self, f):
    if math.fabs(f - math.ceil(f)) < math.fabs(f - math.floor(f)):
        return int(math.ceil(f))
    else:
        return int(math.floor(f))

def IsSegmsIntersect(pt1, pt2, pt3, pt4):
    if GetSignOfPoint(pt1, pt2, pt3) == GetSignOfPoint(pt1, pt2, pt4):
        return 0
    elif GetSignOfPoint(pt3, pt4, pt1) == GetSignOfPoint(pt3, pt4, pt2):
        return 0
    else:
        return 1


def nearestPoint(polyline, point):
    """Computes nearest point on polyline to given point.
    Input:
      polyline - polyline, format is [(x0, y0), (x1, y1), ...]
      point    - point (x, y)
    Output:
      nearest point (x_near, y_near)
    """
    dmin = 3.4e38  # initial approx for minimum distance
    x,y = point[0], point[1]
    xminfin, yminfin = None, None
    for i in range(len(polyline)-1):
        # calculate minimum point:
        x1,y1 = polyline[i][0], polyline[i][1]
        x2,y2 = polyline[i+1][0], polyline[i+1][1]
        dp1p2sq = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2)
        t = ((x-x1)*(x2-x1) + (y-y1)*(y2-y1)) / dp1p2sq
        if t < 0.: t = 0.
        if t > 1.: t = 1.
        xmin, ymin = x1 + t*(x2-x1) , y1 + t*(y2-y1)
        d = math.hypot((x - xmin), (y-ymin))
        if d < dmin:
            dmin = d
            xminfin, yminfin = xmin, ymin
    return (xminfin, yminfin)

def putOnCross(polyline1, polyline2, point):
    """Find crossing point of two polylines nearest to given point
    """
    cross = crossLines(polyline1, polyline2)
    dmin = 3.4e38
    cur_pt = (None, None)
    for p in cross:
        d = math.hypot((p[0] - point[0]), (p[1] - point[1]))
        if d < dmin:
            cur_pt = (p[0], p[1])
            dmin = d
    return cur_pt
    
if __name__ == '__main__':
    print('DEBUG: testing geometry optimization')
    inp = [(0.0, 0.0, 1),
           (1000.0, 0.0,  200),
           (1000.0, 1000.0, 400)
           ]
    outp = opt_geom(inp, 50.0)
    print('DEBUG: input:', inp)
    print('DEBUG: output:', outp)

    print('DEBUG: testing crosses')
    l1 = [(0.0, 0.0), (1.0, 1.0)]
    l2 = [(0.0, 1.0), (1.0, 0.0)]
    cr = crossLines(l1, l2)
    print('DEBUG: input', l1, l2)
    print('DEBUG: output', cr)
    
    l1 = [(0.0, 0.0), (0.5, 1.0), (1.0, 0.0)]
    l2 = [(0.0, 0.5), (1.0, 0.5)]
    cr = crossLines(l1, l2)
    print('DEBUG: input', l1, l2)
    print('DEBUG: output', cr)

    l1 = [(0.0, 0.0), (0.5, 0.4), (1.0, 0.0)]
    l2 = [(0.0, 0.5), (1.0, 0.5)]
    cr = crossLines(l1, l2)
    print('DEBUG: input', l1, l2)
    print('DEBUG: output', cr)

    print('DEBUG: testing getNearestPoint', inp)
    p = (0.0, 0.0)
    pm = nearestPoint(inp, p)
    print(p, pm)

    p = (1000.0, 0.0)
    pm = nearestPoint(inp, p)
    print(p, pm)

    p = (500.0, 500.0)
    pm = nearestPoint(inp, p)
    print(p, pm)

    p = (500.0, 400.0)
    pm = nearestPoint(inp, p)
    print(p, pm)
    
    inp = [(0.0, 0.0), (1000., 1000.)]
    p = (1000.0, 0.0)
    pm = nearestPoint(inp, p)
    print(p, pm)
    
#################################
    print('#########################')
    l = []
    npts = 1000
    r = 5000.
    for i in range(npts):
        a = - math.pi/2. + i * (math.pi / npts)
        pt = [r*math.sin(a)+r, r*math.cos(a), i]
        l.append(pt)
    print()
    print('DEBUG:', len(l), 'points on semi-sircle')
    print()
    print()
    acc = 10.
##    lout = optGeom2D(l, acc)
    lout = opt_geom(l, acc)
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
#################################
    print('#########################')
    l = []
    npts = 100
    ampl = 10.
    stp = 5000./npts
    for i in range(npts):
        if i % 2:
            y = ampl
        else:
            y = 0
        x = stp*i
        l.append((x, y, i))
    print()
    print('DEBUG:', len(l), 'points on zig-zag over x')
    print()
    print()
    acc = 10.
##    lout = optGeom2D(l, acc)
    lout = opt_geom(l, acc)
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
#################################
    print('#########################')
    l = []
    npts = 100
    ampl = 10.
    stp = 5000./npts
    for i in range(npts):
        if i % 2:
            x = ampl
        else:
            x = 0
        y = stp*i
        l.append((x, y, i))
    print()
    print('DEBUG:', len(l), 'points on zig-zag over y')
    print()
    print()
    acc = 10.
##    lout = optGeom2D(l, acc)
    lout = opt_geom(l, acc)
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
################### 3D #########################
    print('######################### 3D ###################')
    l = []
    npts = 4000
    r = 5000.
    dz = 2.
    for i in range(npts):
        a = - math.pi/2. + i * (math.pi / npts)
        pt = [r*math.sin(a)+r, r*math.cos(a), i * dz]
        l.append(pt)
    print()
    print('DEBUG:', len(l), 'points on semi-sircle')
    print()
    print()
    acc = 10.
##    lout = optGeom3D(l, acc)
    lout = opt_geom3(l, acc)
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
    lout = opt_geom3(l) # with default accuracy
    print()
    print()
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
    
#################################
    print('#########################')
    l = []
    npts = 100
    ampl = 10.
    stp = 5000./npts
    for i in range(npts):
        if i % 2:
            x = ampl
            y = -ampl
        else:
            x = 0.
            y = 0.
        z = stp*i
        l.append((x, y, z))
    print()
    print('DEBUG:', len(l), 'points on zig-zag over z')
    print()
    print()
    acc = 15.
##    lout = optGeom3D(l, acc)
    lout = opt_geom3(l, acc)
    print('DEBUG: result', lout)
    print('DEBUG:', len(lout), 'points')
