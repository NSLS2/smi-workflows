from prefect import flow, task, get_run_logger
from pathlib import Path
from tiled.client import from_uri
from prefect.blocks.system import Secret
from prefect.states import Failed
from data_validation import get_run

import event_model
import tqdm
import shutil


# Optional "best guess" aliases for known motor short-names.  These are only
# consulted when the exact key is missing from the event data.  Add entries
# here if/when you want a short name (e.g. "x") to resolve to a real signal
# (e.g. "piezo_x").  Leaving this empty keeps behaviour purely literal.
SAFE_KEY_ALIASES: dict[str, str] = {
    # "x": "piezo_x",
    # "y": "piezo_y",
}


class _SafeFormatPlaceholder:
    """
    Stand-in for a missing format key.

    When formatted it reproduces the original ``{key}`` (preserving any
    format spec, e.g. ``{N:06d}``) so unresolved placeholders are written
    out literally instead of raising ``KeyError``.
    """

    def __init__(self, key: str):
        self.key = key

    def __format__(self, spec: str) -> str:
        if spec:
            return "{" + self.key + ":" + spec + "}"
        return "{" + self.key + "}"

    def __str__(self) -> str:
        return "{" + self.key + "}"

    def __repr__(self) -> str:
        return self.__str__()

    def __ascii__(self) -> str:
        return self.__str__()


class _SafeFormatDict(dict):
    """A mapping for ``str.format_map`` that never raises on missing keys.

    Missing keys are recorded in ``missing_keys`` (after trying the alias
    table) and substituted with a literal ``{key}`` placeholder.
    """

    def __init__(self, *args, aliases=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.aliases = aliases or {}
        self.missing_keys: set[str] = set()

    def __missing__(self, key):
        alias = self.aliases.get(key)
        if alias is not None and alias in self:
            return self[alias]
        self.missing_keys.add(key)
        return _SafeFormatPlaceholder(key)


def _safe_format(template, mapping, *, logger=None):
    """
    ``str.format``-style substitution that leaves unresolved ``{name}``
    placeholders in the output instead of raising ``KeyError``.

    Returns the formatted string.  Any keys that could not be resolved are
    logged as a warning (if a logger is provided).
    """
    safe = _SafeFormatDict(mapping, aliases=SAFE_KEY_ALIASES)
    result = template.format_map(safe)
    if safe.missing_keys and logger is not None:
        logger.warning(
            f"Unresolved name(s) {sorted(safe.missing_keys)} in template "
            f"{template!r}; left literally in output: {result!r}"
        )
    return result


@task
def do_symlinking(
    links: list[tuple[str, Path, Path]],
    overwrite_dest=False,
) -> tuple[list[tuple[str, Path, Path]], list[tuple[str, Path, Path]]]:
    """
    Create the symlinks, making target directories as needed.

    Paramaters
    ----------
    links : list of (uid, src, dest) tuples
        The uid, source file and destination files

    overwrite_dest : bool, optional
        If an existing destitation should be unlinked and replaced.

    Returns
    -------
    linked, failed : list of (uid, src, dest) tuples
        The linked (or failed) values.
    """

    logger = get_run_logger()
    failed = []
    linked = []

    for uid, src, dest, analysis in tqdm.tqdm(links, leave=False):
        logger.info(f"uid: {uid} src: {src} dest: {dest} analysis: {analysis}")
        if not src.exists():
            logger.error(f"{src} does not exist. uid: {uid} dest: {dest} analysis: {analysis}")
            failed.append((uid, src, dest, analysis))
            continue

        try:
            dest.parent.mkdir(exist_ok=True, parents=True)

            if not analysis.exists():
                # copy the default analysis notebooks to the analysis directory
                default_analysis_path_s = Path('/nsls2/data/smi/shared/default_nb/saxs.ipynb')
                default_analysis_path_w = Path('/nsls2/data/smi/shared/default_nb/waxs.ipynb')
                
                analysis.mkdir(exist_ok=True, parents=True)

                shutil.copyfile(default_analysis_path_s, analysis / 'saxs.ipynb')
                shutil.copyfile(default_analysis_path_w, analysis / 'waxs.ipynb')
                



            if overwrite_dest and dest.exists():
                dest.unlink()
            dest.symlink_to(src)
            logger.info(f"symlink: {src} to {dest}")

        except Exception as e:
            tqdm.tqdm.write(f"FAILED: {dest}")
            logger.exception(f"Exception while making symlink: {src} to {dest}")
            failed.append((uid, src, dest, analysis))
        else:
            tqdm.tqdm.write(f"Linked: {dest}")
            linked.append((uid, src, dest, analysis))
    if failed:
        logger.error(f"Tasks failed: {failed}")
    logger.info("Linked items:")
    for item in linked:
        logger.info(item)
    return linked, failed


@task
def get_symlink_pairs(ref, *, det_map, root_map=None, api_key=None, dry_run=False):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document
    det_map : dict[str, str]
        A dictionaly mapping the detector name (1M, 900KW)
        to the type of measurement (SAXS, WAXS)
    root_map : dict[str, str], optional
        A mapping of root in the resource document -> a new path
        as in databroker

    Returns
    -------
    list[tuple[str, Path, Path]]
         A tuple of the start uid, the source path and the destination path.
    """
    logger = get_run_logger()
    ########################
    if root_map is None:
        root_map = {}

    links = []
    target_template: str
    output_path: str
    resource_info = {}
    datum_info = {}
    target_keys = set()
    ########################

    # hrf = db[ref]
    hrf = get_run(ref, api_key=api_key)
    for name, doc in hrf.documents():
        if name == "start":
            start_uid = doc["uid"]
            #target_template = (f"{{det_name}}/{doc['username']}_{doc['sample_name']}_"
            #                   f"id{doc['scan_id']}_{{N:06d}}_{{det_type}}.tif")
            target_template = (f"{{det_name}}/{doc['sample_name']}_"
                               f"id{doc['scan_id']}_{{N:06d}}_{{det_type}}.tif")

            target_path = Path(
                (f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}/"
                f"projects/{doc['project_name']}/user_data")
            )
            analysis_path = Path(
                (f"/nsls2/data/smi/proposals/{doc['cycle']}/{doc['data_session']}/"
                f"projects/{doc['project_name']}/analysis")
            )

        elif name == "resource":

            if doc["spec"] != "AD_TIFF":
                continue
            doc_root = doc["root"]
            resource_info[doc["uid"]] = {
                "path": Path(root_map.get(doc_root, doc_root)) / doc["resource_path"],  # noqa: 501
                "kwargs": doc["resource_kwargs"],
            }
        elif "datum" in name:
            if name == "datum":
                doc = event_model.pack_datum_page(doc)

            for datum_uid, point_number in zip(
                doc["datum_id"], doc["datum_kwargs"]["point_number"]
            ):
                datum_info[datum_uid] = (
                    resource_info[doc["resource"]],
                    point_number,
                )

        elif name == "descriptor":
            for k, v in doc["data_keys"].items():
                if "external" in v:
                    target_keys.add(k)
        elif "event" in name:
            # continue building the target_template here adding
            # the event level things (motor positions)
            if name == "event":
                doc = event_model.pack_event_page(doc)
            single_doc_data = {key:doc['data'][key][0] for key in doc['data']}
            for key in target_keys:

                det, _, _ = key.partition("_")
                det_name = det.removeprefix("pil")
                det_type = det_map.get(det_name, det_name)

                if key not in doc["data"]:
                    continue

                for datum_id in doc["data"][key]:
                    # pulling out the image column
                    resource_vals, point_number = datum_info[datum_id]
                    orig_template = resource_vals["kwargs"]["template"]
                    fpp = resource_vals["kwargs"]["frame_per_point"]
                    base_fname = resource_vals["kwargs"]["filename"]

                    for fr in range(fpp):
                        source_path = Path(
                            orig_template
                            % (
                                str(resource_vals["path"]) + "/",
                                base_fname,
                                point_number * fpp + fr,
                            )
                        )


                        # Two passes so placeholders that only appear after
                        # the first substitution (e.g. a sample_name written
                        # as "{{x}}") still get resolved.  Both passes are
                        # "safe": any name that cannot be resolved from the
                        # event data is left literally in the filename
                        # instead of raising KeyError and killing the flow.
                        format_data = {
                            "det_name": det_name,
                            "N": point_number * fpp + fr,
                            "det_type": det_type,
                            **single_doc_data,
                        }
                        # Only the second pass warns: its input contains
                        # every literal {key} the first pass left behind, so
                        # its missing-key set is a superset of the first
                        # pass's.  This avoids emitting duplicate warnings
                        # for the same unresolved name.
                        dest_name = _safe_format(target_template, format_data)
                        dest_name = _safe_format(
                            dest_name, format_data, logger=logger
                        )
                        dest_path = target_path / dest_name

                        links.append(
                            (start_uid, source_path, dest_path, analysis_path)
                        )

        elif name == "stop":
            break

    if not dry_run:
        linked, failed = do_symlinking(links, overwrite_dest=True)
    else:
        logger.info("Dry run: skipped link information: ")
        for link in links:
            logger.info(f"UID: {link[0]} src: {link[1]} dest: {link[2]} analysis: {link[3]}")
        return

    if len(failed) > 0:
        logger.info(f"Failed generating links {failed}")
        success_rate = len(linked) / (len(failed) + len(linked)) * 100
        logger.info(f"Success rate: {success_rate:.2f}%")
        return Failed(message=f"{len(failed)} failures - {success_rate:.2f}% success rate")
    elif len(linked) > 0:
        logger.info(f"Links successfully generated {linked}")
        logger.info(f"Success rate: 100%")
        return

