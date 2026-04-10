// Bokeh CustomJS callbacks for the obsloctap skymap page.
// Each function is called from a callback entry point at the bottom of the
// injected script; Bokeh-provided variables (cb_obj, etc.) are passed explicitly
// so the logic here is independent of Bokeh's implicit scope.

function galToggle(cb_obj, coord_sel, r_gal_eq) {
    const gal = coord_sel !== null && coord_sel.active === 1;
    const on = cb_obj.active;
    r_gal_eq.visible = on && !gal;
    // galactic mode: no line (plane = b=0 grid equator)
}

function eclToggle(cb_obj, coord_sel, r_ecl_eq, r_ecl_gal) {
    const gal = coord_sel !== null && coord_sel.active === 1;
    const on = cb_obj.active;
    r_ecl_eq.visible  = on && !gal;
    r_ecl_gal.visible = on && gal;
}

function coordSelect(cb_obj, src, t_gal, t_ecl,
                     r_gal_eq, r_ecl_eq, r_ecl_gal,
                     r_ax_bot_eq, r_ax_side_eq,
                     r_ax_bot_gal, r_ax_side_gal) {
    const gal = cb_obj.active === 1;
    const sfx = gal ? 'gal' : 'eq';
    const d = src.data;
    d['x'] = Array.from(d['x_' + sfx]);
    d['y'] = Array.from(d['y_' + sfx]);
    src.change.emit();
    // One plane renderer per system; respect toggle active state.
    t_gal.disabled = gal;
    r_gal_eq.visible  = !gal && t_gal.active;
    r_ecl_eq.visible  = !gal && t_ecl.active;
    r_ecl_gal.visible =  gal && t_ecl.active;
    r_ax_bot_eq.visible  = !gal;  r_ax_side_eq.visible  = !gal;
    r_ax_bot_gal.visible =  gal;  r_ax_side_gal.visible =  gal;
}

function plotFilter(src, band_cb, band_names, nexp_spinner,
                    status_select, target_select, filters, renderers) {
    const activeBands = new Set(
        band_cb.active.map((index) => band_names[index])
    );
    const threshold = Number(nexp_spinner.value) || 1;
    const selectedStatus = status_select.value;
    const selectedTarget = target_select.value;
    const bands = src.data['band'];
    const nexp = src.data['nexp_num'];
    const statuses = src.data['status'];
    const targets = src.data['target'];
    for (let i = 0; i < filters.length; i++) {
        const band = band_names[i];
        renderers[i].visible = activeBands.has(band);
        const mask = [];
        for (let j = 0; j < bands.length; j++) {
            const statusOk =
                selectedStatus === 'All' || statuses[j] === selectedStatus;
            const targetOk =
                selectedTarget === 'All' || targets[j] === selectedTarget;
            mask.push(
                activeBands.has(bands[j]) &&
                bands[j] === band &&
                Number(nexp[j]) >= threshold &&
                statusOk &&
                targetOk
            );
        }
        filters[i].booleans = mask;
        filters[i].change.emit();
    }
    src.change.emit();
}

function pdfLinkSync(coord_sel, t_gal, t_ecl) {
    const link = document.getElementById('pdf-download-link');
    if (!link) {
        return;
    }
    const url = new URL(link.href, window.location.href);
    const galactic = coord_sel !== null && coord_sel.active === 1;
    url.searchParams.set('coordinates', galactic ? 'galactic' : 'equatorial');
    url.searchParams.set('galactic_plane', String(!galactic && t_gal.active));
    url.searchParams.set('ecliptic_plane', String(t_ecl.active));
    link.href = url.toString();
}