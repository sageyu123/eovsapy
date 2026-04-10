"""Smooth complex-phase helpers for browser in-band diagnostics."""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np


TWO_PI = 2.0 * np.pi


def _as_1d_float(values: np.ndarray, name: str) -> np.ndarray:
    """Return one-dimensional float data or raise."""

    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1:
        raise ValueError("{0} must be 1D; got shape {1}.".format(name, arr.shape))
    return arr


def _as_1d_complex(values: np.ndarray, name: str) -> np.ndarray:
    """Return one-dimensional complex data or raise."""

    arr = np.asarray(values, dtype=np.complex128)
    if arr.ndim != 1:
        raise ValueError("{0} must be 1D; got shape {1}.".format(name, arr.shape))
    return arr


def weighted_polyfit(
    x: np.ndarray,
    y: np.ndarray,
    w: np.ndarray,
    deg: int,
    ridge: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Weighted polynomial fit with optional ridge regularization."""

    x = _as_1d_float(x, "x")
    y = _as_1d_float(y, "y")
    w = _as_1d_float(w, "w")
    if x.shape != y.shape or x.shape != w.shape:
        raise ValueError("x, y, and w must have the same shape.")
    degree = int(deg)
    if degree < 0:
        raise ValueError("deg must be >= 0.")
    reg = float(ridge)
    if reg < 0.0:
        raise ValueError("ridge must be >= 0.")
    matrix = np.vander(x, N=degree + 1, increasing=True)
    sqrt_w = np.sqrt(np.clip(w, 0.0, np.inf))
    matrix_w = matrix * sqrt_w[:, None]
    y_w = y * sqrt_w
    lhs = matrix_w.T @ matrix_w
    rhs = matrix_w.T @ y_w
    if reg > 0.0:
        penalty = np.eye(degree + 1, dtype=float) * reg
        penalty[0, 0] = 0.0
        lhs = lhs + penalty
    coeffs = np.linalg.solve(lhs, rhs)
    return coeffs, matrix @ coeffs


def _difference_matrix(order: int, size: int) -> np.ndarray:
    """Return a finite-difference matrix of the requested order."""

    order = int(order)
    if order < 1:
        raise ValueError("order must be >= 1.")
    if size <= order:
        return np.zeros((0, size), dtype=float)
    matrix = np.eye(size, dtype=float)
    for _ in range(order):
        matrix = np.diff(matrix, axis=0)
    return matrix


def _penalized_complex_smoother(
    vis: np.ndarray,
    weights: np.ndarray,
    *,
    curvature_penalty: float,
    slope_penalty: float,
) -> np.ndarray:
    """Smooth one complex segment with derivative penalties.

    The fit minimizes, separately for real and imaginary parts:

        sum_i w_i (z_i - y_i)^2
        + slope_penalty * ||D1 z||^2
        + curvature_penalty * ||D2 z||^2

    This keeps the model itself smooth while strongly discouraging
    piecewise-linear or staircase-like gradients.
    """

    vis = _as_1d_complex(vis, "vis")
    weights = _as_1d_float(weights, "weights")
    if vis.shape != weights.shape:
        raise ValueError("vis and weights must have the same shape.")
    size = vis.size
    if size < 3:
        return vis.copy()
    wdiag = np.diag(np.clip(weights, 0.0, np.inf))
    d1 = _difference_matrix(1, size)
    d2 = _difference_matrix(2, size)
    lhs = wdiag.copy()
    if d1.size:
        lhs = lhs + float(slope_penalty) * (d1.T @ d1)
    if d2.size:
        lhs = lhs + float(curvature_penalty) * (d2.T @ d2)
    # Small diagonal loading for numerical stability on sparse/flat segments.
    lhs = lhs + np.eye(size, dtype=float) * 1e-8
    rhs_re = np.clip(weights, 0.0, np.inf) * vis.real
    rhs_im = np.clip(weights, 0.0, np.inf) * vis.imag
    fit_re = np.linalg.solve(lhs, rhs_re)
    fit_im = np.linalg.solve(lhs, rhs_im)
    return fit_re + 1j * fit_im


def fit_smooth_bandpass_phase_complex(
    freq_hz: np.ndarray,
    vis_fit: np.ndarray,
    *,
    mask: np.ndarray,
    weights: Optional[np.ndarray] = None,
    method: str = "smooth",
    deg: int = 7,
    ridge: float = 0.0,
    gap_hz: float = 5e8,
    rmax_rad: float = 2.5,
    amin: float = 0.0,
    n_iter: int = 2,
    dly_res_s: float = 0.0,
    phi0_rad: float = 0.0,
    curvature_penalty: float = 120.0,
    slope_penalty: float = 2.0,
) -> Dict[str, Any]:
    """Fit a smooth complex-phase model without phase unwrapping.

    The input visibilities are expected to already include the active mean
    in-band delay correction. The helper optionally removes one residual linear
    delay/phase term, fits a smooth complex bandpass in the deramped space, and
    returns the wrapped model and wrapped residuals for display.
    """

    freq_hz = _as_1d_float(freq_hz, "freq_hz")
    vis_fit = _as_1d_complex(vis_fit, "vis_fit")
    if freq_hz.shape != vis_fit.shape:
        raise ValueError("freq_hz and vis_fit must have the same shape.")
    mask_fit = np.asarray(mask, dtype=bool)
    if mask_fit.shape != freq_hz.shape:
        raise ValueError("mask must have the same shape as freq_hz.")
    if weights is None:
        weights = np.ones(freq_hz.shape, dtype=float)
    weights = _as_1d_float(weights, "weights")
    if weights.shape != freq_hz.shape:
        raise ValueError("weights must have the same shape as freq_hz.")

    model_kind = str(method).strip().lower()
    if model_kind not in ("poly", "smooth"):
        raise ValueError("method must be 'poly' or 'smooth'.")
    iterations = max(int(n_iter), 1)
    amp_min = float(amin)
    gap_hz = float(gap_hz)
    ramp = np.exp(-1j * (TWO_PI * freq_hz * float(dly_res_s) + float(phi0_rad)))
    vis_deramped = vis_fit * ramp

    valid = (
        mask_fit
        & np.isfinite(freq_hz)
        & np.isfinite(vis_fit.real)
        & np.isfinite(vis_fit.imag)
        & np.isfinite(weights)
        & (weights > 0.0)
    )
    if amp_min > 0.0:
        valid = valid & (np.abs(vis_fit) > amp_min)
    if np.count_nonzero(valid) < 3:
        blank = np.full(freq_hz.shape, np.nan + 1j * np.nan, dtype=np.complex128)
        blank_phase = np.full(freq_hz.shape, np.nan, dtype=float)
        return {
            "V0": vis_deramped,
            "V0_fit": blank,
            "V_fit_model": blank,
            "phi_model_wrapped": blank_phase,
            "phi_res_wrapped": blank_phase,
            "mask_fit": valid,
            "diagnostics": {
                "method": model_kind,
                "status": "not_enough_points",
                "n_used": int(np.count_nonzero(valid)),
            },
        }

    def _fit_poly_complex(current_mask: np.ndarray) -> np.ndarray:
        idx = np.where(current_mask)[0]
        nu = freq_hz[idx]
        ww = weights[idx]
        vis = vis_deramped[idx]
        nu0 = float(np.nanmedian(nu))
        span = float(np.nanmax(nu) - np.nanmin(nu))
        if not np.isfinite(span) or span <= 0.0:
            span = 1.0
        xvals = (nu - nu0) / span
        _, re_fit = weighted_polyfit(xvals, vis.real, ww, deg=int(deg), ridge=float(ridge))
        _, im_fit = weighted_polyfit(xvals, vis.imag, ww, deg=int(deg), ridge=float(ridge))
        out = np.full(freq_hz.shape, np.nan + 1j * np.nan, dtype=np.complex128)
        out[idx] = re_fit + 1j * im_fit
        return out

    def _fit_smooth_complex(current_mask: np.ndarray) -> np.ndarray:
        idx = np.where(current_mask)[0]
        order = np.argsort(freq_hz[idx])
        idx = idx[order]
        nu = freq_hz[idx]
        vis = vis_deramped[idx]
        ww = weights[idx]
        cuts = np.where(np.diff(nu) > gap_hz)[0] + 1
        segments = np.split(np.arange(idx.size), cuts)
        out = np.full(freq_hz.shape, np.nan + 1j * np.nan, dtype=np.complex128)
        for segment in segments:
            if segment.size < 3:
                out[idx[segment]] = vis[segment]
                continue
            local_vis = vis[segment]
            local_weights = ww[segment]
            seg_len = max(segment.size, 3)
            curve_pen = float(curvature_penalty) * (seg_len / 25.0) ** 2
            slope_pen = float(slope_penalty) * max(seg_len / 25.0, 1.0)
            try:
                out[idx[segment]] = _penalized_complex_smoother(
                    local_vis,
                    local_weights,
                    curvature_penalty=curve_pen,
                    slope_penalty=slope_pen,
                )
            except np.linalg.LinAlgError:
                out[idx[segment]] = local_vis
        return out

    current = valid.copy()
    vis_smooth = np.full(freq_hz.shape, np.nan + 1j * np.nan, dtype=np.complex128)
    for _ in range(iterations):
        vis_smooth = _fit_poly_complex(current) if model_kind == "poly" else _fit_smooth_complex(current)
        residual = np.angle(vis_deramped * np.conj(vis_smooth))
        next_mask = current & np.isfinite(residual) & (np.abs(residual) <= float(rmax_rad))
        if np.array_equal(next_mask, current):
            current = next_mask
            break
        current = next_mask

    if np.count_nonzero(current) >= 3:
        vis_smooth = _fit_poly_complex(current) if model_kind == "poly" else _fit_smooth_complex(current)
    else:
        vis_smooth[:] = np.nan + 1j * np.nan

    model = vis_smooth * np.exp(1j * (TWO_PI * freq_hz * float(dly_res_s) + float(phi0_rad)))
    return {
        "V0": vis_deramped,
        "V0_fit": vis_smooth,
        "V_fit_model": model,
        "phi_model_wrapped": np.angle(model),
        "phi_res_wrapped": np.angle(vis_fit * np.conj(model)),
        "mask_fit": current,
        "diagnostics": {
            "method": model_kind,
            "status": "ok" if np.count_nonzero(current) >= 3 else "not_enough_points_after_rejection",
            "n_used": int(np.count_nonzero(current)),
            "gap_hz": float(gap_hz),
            "curvature_penalty": float(curvature_penalty),
            "slope_penalty": float(slope_penalty),
        },
    }


def wrapped_line_segments(
    xvals: np.ndarray,
    phase_wrapped: np.ndarray,
    *,
    band_id: Optional[np.ndarray] = None,
    mask: Optional[np.ndarray] = None,
    gap_x: Optional[float] = None,
) -> list[dict[str, np.ndarray]]:
    """Split wrapped phase lines at discontinuities and band gaps."""

    xvals = _as_1d_float(xvals, "xvals")
    phase_wrapped = _as_1d_float(phase_wrapped, "phase_wrapped")
    if xvals.shape != phase_wrapped.shape:
        raise ValueError("xvals and phase_wrapped must have the same shape.")
    current_mask = np.isfinite(xvals) & np.isfinite(phase_wrapped)
    if mask is not None:
        mask_arr = np.asarray(mask, dtype=bool)
        if mask_arr.shape != xvals.shape:
            raise ValueError("mask must have the same shape as xvals.")
        current_mask = current_mask & mask_arr
    if band_id is None:
        band_id = np.zeros(xvals.shape, dtype=int)
    band_id = np.asarray(band_id)
    if band_id.shape != xvals.shape:
        raise ValueError("band_id must have the same shape as xvals.")

    out: list[dict[str, np.ndarray]] = []
    for band in np.unique(band_id[current_mask]):
        idx0 = np.where((band_id == band) & current_mask)[0]
        if idx0.size < 2:
            continue
        order = np.argsort(xvals[idx0])
        idx = idx0[order]
        xx = xvals[idx]
        yy = phase_wrapped[idx]
        split_points = [np.where(np.abs(np.diff(yy)) > np.pi)[0] + 1]
        if gap_x is not None:
            split_points.append(np.where(np.diff(xx) > float(gap_x))[0] + 1)
        breaks = np.unique(np.concatenate(split_points)) if split_points else np.array([], dtype=int)
        for segment in np.split(np.arange(idx.size), breaks):
            if segment.size < 2:
                continue
            out.append(
                {
                    "x": np.asarray(xx[segment], dtype=float),
                    "y": np.asarray(yy[segment], dtype=float),
                }
            )
    return out
