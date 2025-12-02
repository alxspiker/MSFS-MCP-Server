import anyio
import asyncio
import contextlib
import logging
import time
import math
import uuid
import csv
import os
from typing import Optional, Any, Dict, List, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

from SimConnect import SimConnect, AircraftRequests, AircraftEvents
from mcp.server.fastmcp import FastMCP

from enum import Enum

log = logging.getLogger("msfs_mcp")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class BlackboxLogger:
    def __init__(self, filename="flight_data_log.csv"):
        self.filename = filename
        self.headers = [
            "timestamp", "phase", "ias", "ground_speed", "altitude", 
            "heading", "target_heading", "heading_error", 
            "rudder_cmd", "p_term", "i_term", "d_term", 
            "pitch", "pitch_rate", "elevator_cmd", 
            "bank", "aileron_cmd", "on_ground", "beta",
            "action", "detail"
        ]
        self._initialize_file()

    def _initialize_file(self):
        # Overwrite existing file with headers
        try:
            with open(self.filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
            log.info(f"Blackbox logger initialized: {self.filename}")
        except Exception as e:
            log.error(f"Failed to initialize blackbox logger: {e}")

    def log(self, data: Dict[str, Any]):
        try:
            row = []
            for h in self.headers:
                row.append(data.get(h, ""))
            
            with open(self.filename, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except Exception:
            pass

    def log_action(self, base: Dict[str, Any], action: str, detail: str = ""):
        payload = dict(base)
        payload["action"] = action
        payload["detail"] = detail
        self.log(payload)

# Initialize global logger on server start
blackbox = BlackboxLogger()



class _SuppressClosedResourceError(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - logging filter
        if "Error in message router" in record.getMessage():
            exc = record.exc_info[1] if record.exc_info else None
            if isinstance(exc, anyio.ClosedResourceError):
                return False
        return True


logging.getLogger("mcp.server.streamable_http").addFilter(_SuppressClosedResourceError())

mcp = FastMCP(
    name="msfs-mcp-server",
    stateless_http=True,
    streamable_http_path="/",
    json_response=True,
)

# --------- Which SimVars to dump in get_state --------- #
# Extend this list with any SimVars you care about.
# STATE_SIMVARS # Instead we will loop through all sim variables


# --------- JSON-RPC models --------- #

class JsonRpcRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Any] = None
    method: str
    params: Optional[Dict[str, Any]] = None


class JsonRpcResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[Any]
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None


# --------- SimConnect wiring --------- #

class MsfsClient:
    def __init__(self):
        self.sm: Optional[SimConnect] = None
        self.aq: Optional[AircraftRequests] = None
        self.ae: Optional[AircraftEvents] = None

    def connect(self):
        if self.sm is not None:
            return
        log.info("Connecting to MSFS via SimConnect...")
        self.sm = SimConnect()
        self.aq = AircraftRequests(self.sm, _time=0)  # no caching
        self.ae = AircraftEvents(self.sm)
        log.info("Connected to SimConnect")

    def _ensure_connected(self):
        if self.sm is None:
            self.connect()

    def _events(self) -> AircraftEvents:
        self._ensure_connected()
        return self.ae

    def _reqs(self) -> AircraftRequests:
        self._ensure_connected()
        return self.aq

    @staticmethod
    def _attribute_map(container: Any) -> Dict[int, List[str]]:
        mapping: Dict[int, List[str]] = {}
        for attr_name, attr_value in vars(container).items():
            mapping.setdefault(id(attr_value), []).append(attr_name)
        return mapping

    @staticmethod
    def _helper_metadata(helper: Any, attr_map: Dict[int, List[str]]) -> Dict[str, Any]:
        class_name = helper.__class__.__name__.lstrip("_")
        attr_names = [
            name for name in attr_map.get(id(helper), []) if not name.startswith("_")
        ]
        aliases = sorted({class_name, *attr_names}, key=str.lower)
        return {
            "class": class_name,
            "attribute": attr_names[0] if attr_names else None,
            "aliases": aliases,
        }

    @staticmethod
    def _decode_event_tuple(event_tuple: Tuple[Any, ...]) -> Dict[str, Any]:
        raw_name = event_tuple[0]
        event_name = (
            raw_name.decode("utf-8")
            if isinstance(raw_name, (bytes, bytearray))
            else str(raw_name)
        )
        description = event_tuple[1] if len(event_tuple) > 1 else ""
        detail = event_tuple[2] if len(event_tuple) > 2 else ""
        return {
            "event": event_name,
            "description": description,
            "detail": detail,
        }

    @staticmethod
    def _serialize_event_entries(helper: Any) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        for event_tuple in getattr(helper, "list", []):
            if not event_tuple:
                continue
            events.append(MsfsClient._decode_event_tuple(event_tuple))
        return events

    @staticmethod
    def _find_event_entry(helper: Any, target_event: str) -> Optional[Dict[str, Any]]:
        target = target_event.strip().lower()
        if not target:
            return None
        for event_tuple in getattr(helper, "list", []):
            event_info = MsfsClient._decode_event_tuple(event_tuple)
            if event_info["event"].lower() == target:
                return event_info
        return None

    @staticmethod
    def _coerce_value(value: Any) -> Any:
        """Convert SimConnect return types to JSON-serializable primitives."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").rstrip("\x00")
        if isinstance(value, (str, bool, int, float)):
            return value
        if isinstance(value, dict):
            return {k: MsfsClient._coerce_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [MsfsClient._coerce_value(v) for v in value]
        inner = getattr(value, "value", None)
        if inner is not None and inner is not value:
            return MsfsClient._coerce_value(inner)
        return value

    # ---- programmatic state dump ---- #

    def get_state(
        self,
        as_lines: bool = False,
        group: Optional[str] = None,
        include_groups: bool = False,
    ):
        """
        Programmatic state dump: iterate every AircraftRequests helper group and read
        each non-indexed SimVar via helper.json().

        Passing group is required and selects a single helper category (for example
        "AircraftEngineData" or "EngineData"). Calls without a group, or with an
        unknown group, return only the list of available groups. When
        include_groups=True (and as_lines=False) the response will be
        {"state": {...}, "groups": [...]} so callers can see the values and the
        catalog simultaneously. When as_lines=True, include_groups is ignored and
        missing/invalid groups yield a newline-separated list of available groups.
        """
        aq = self._reqs()

        state: Dict[str, Any] = {}

        target_group = group.strip().lower() if group else None
        groups_metadata: Optional[List[Dict[str, Any]]] = None

        def ensure_groups_metadata() -> List[Dict[str, Any]]:
            nonlocal groups_metadata
            if groups_metadata is None:
                groups_metadata = self.list_state_groups()
            return groups_metadata

        if include_groups and not as_lines:
            ensure_groups_metadata()

        def group_list_response(message: str):
            groups = ensure_groups_metadata()
            alias_set = {
                alias for entry in groups for alias in entry.get("aliases", [])
            }
            alias_list = sorted(alias_set, key=str.lower)
            if as_lines:
                return "\n".join(alias_list)
            return {
                "message": message,
                "groups": alias_list,
                "groups_text": ", ".join(alias_list),
                "group_details": groups,
            }

        if not target_group:
            return group_list_response("group parameter required")

        helper = self._find_helper_by_group(aq, target_group)
        if helper is None:
            return group_list_response(f"Unknown group '{group}'")

        try:
            # helper_state = helper.json()
            # Custom implementation to handle indexed variables (defaulting to index 1)
            helper_state = {}
            for name in getattr(helper, "list", {}):
                req = getattr(helper, name)
                if req is None:
                    continue
                
                if ":index" in name:
                    try:
                        # Default to index 1 for now
                        req.setIndex(1)
                    except Exception:
                        pass
                
                try:
                    val = req.value
                    if val is not None:
                        helper_state[name] = val
                except Exception:
                    pass
            
            # Polyfill: If we have indexed combustion data saying TRUE, force the legacy non-indexed variable to TRUE
            # This helps AIs that might only look at the legacy variable.
            if helper_state.get("GENERAL_ENG_COMBUSTION:index") == 1.0:
                helper_state["ENG_COMBUSTION"] = 1.0

        except Exception as exc:  # pragma: no cover - defensive: SimConnect internals
            log.debug(
                "Failed to read SimVars from %s: %s",
                helper.__class__.__name__,
                exc,
            )
            raise

        for name, raw_value in helper_state.items():
            value = self._coerce_value(raw_value)
            if value is not None:
                state[name] = value

        if as_lines:
            if include_groups:
                log.warning("Ignoring include_groups when as_lines=True")
            lines = [f"{name}: {state[name]}" for name in sorted(state.keys())]
            return "\n".join(lines)

        if include_groups and groups_metadata is not None:
            return {"state": state, "groups": groups_metadata}

        return state

    def list_state_groups(self) -> List[Dict[str, Any]]:
        """Return metadata about every AircraftRequests helper group."""
        aq = self._reqs()
        attr_map = self._attribute_map(aq)

        groups: List[Dict[str, Any]] = []
        for helper in getattr(aq, "list", []):
            groups.append(self._helper_metadata(helper, attr_map))

        return groups

    def list_event_groups(self, include_events: bool = False) -> List[Dict[str, Any]]:
        """Return metadata (and optionally events) for every AircraftEvents helper."""
        ae = self._events()
        attr_map = self._attribute_map(ae)

        groups: List[Dict[str, Any]] = []
        for helper in getattr(ae, "list", []):
            meta = self._helper_metadata(helper, attr_map)
            helper_events = getattr(helper, "list", [])
            if include_events:
                meta["events"] = self._serialize_event_entries(helper)
            else:
                meta["event_count"] = len(helper_events)
            groups.append(meta)

        return groups

    def get_event_group(self, group: str) -> Dict[str, Any]:
        ae = self._events()
        target_group = group.strip().lower() if group else None
        if not target_group:
            raise ValueError("group parameter required")

        helper = self._find_helper_by_group(ae, target_group)
        if helper is None:
            raise ValueError(f"Unknown event group '{group}'")

        attr_map = self._attribute_map(ae)
        meta = self._helper_metadata(helper, attr_map)
        meta["events"] = self._serialize_event_entries(helper)
        return meta

    def set_event_state(
        self,
        group: str,
        event: str,
        value: Optional[int] = 0,
    ) -> Dict[str, Any]:
        ae = self._events()
        target_group = group.strip().lower() if group else None
        if not target_group:
            raise ValueError("group parameter required")

        helper = self._find_helper_by_group(ae, target_group)
        if helper is None:
            raise ValueError(f"Unknown event group '{group}'")

        target_event = event.strip() if event else ""
        if not target_event:
            raise ValueError("event parameter required")

        event_info = self._find_event_entry(helper, target_event)
        if event_info is None:
            raise ValueError(f"Unknown event '{event}' in group '{group}'")

        event_callable = getattr(helper, event_info["event"], None)
        if event_callable is None:
            raise ValueError(f"Event '{event_info['event']}' is not callable")

        payload = 0 if value is None else int(value)
        event_callable(payload)

        attr_map = self._attribute_map(ae)
        meta = self._helper_metadata(helper, attr_map)
        return {
            "status": "ok",
            "group": meta,
            "event": event_info,
            "value": payload,
        }

    def set_event_states(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply a list of event operations sequentially."""
        if not isinstance(operations, list) or not operations:
            raise ValueError("operations must be a non-empty list")

        results: List[Dict[str, Any]] = []
        for idx, op in enumerate(operations):
            if not isinstance(op, dict):
                raise ValueError(f"operation {idx} must be an object")
            group = op.get("group")
            event = op.get("event")
            value = op.get("value")
            if not group or not event:
                raise ValueError(f"operation {idx} requires group and event")
            result = self.set_event_state(group, event, value)
            result["operation_index"] = idx
            results.append(result)

        return results

    @staticmethod
    def _find_helper_by_group(
        container: Any, target_group: str
    ) -> Optional[Any]:
        for helper in getattr(container, "list", []):
            if MsfsClient._helper_matches(container, helper, target_group):
                return helper
        return None

    @staticmethod
    def _helper_matches(container: Any, helper: Any, target_group: str) -> bool:
        """Check if a helper instance matches the requested group name."""
        helper_type = helper.__class__.__name__.lstrip("_").lower()
        if helper_type == target_group:
            return True

        for attr_name, attr_value in vars(container).items():
            if attr_value is helper and attr_name.lower() == target_group:
                return True

        return False

    def get_simvar(self, name: str, units: Optional[str] = None):
        """
        Convenience wrapper around AircraftRequests.get, returning a JSON-safe primitive.
        """
        aq = self._reqs()
        # AircraftRequests.get() signature: get(name) - units are not passed directly
        # The SimConnect library handles units internally based on the SimVar name
        raw = aq.get(name)
        return self._coerce_value(raw)

    def fire_event(self, event_name: str, value: int = 0):
        """
        Convenience wrapper to send a single SimConnect event by name.
        This is used internally by the autopilot loops.
        Uses AircraftEvents.find() to locate the event dynamically.
        """
        ae = self._events()
        event_callable = ae.find(event_name)
        if event_callable is None:
            raise ValueError(f"Unknown SimConnect event: {event_name}")
        event_callable(int(value))


class AutopilotMode(str, Enum):
    TAKEOFF = "takeoff"
    LANDING = "landing"


class AutopilotStatus(BaseModel):
    id: str
    mode: AutopilotMode
    active: bool
    phase: str
    last_update: float
    extra: Dict[str, Any] = Field(default_factory=dict)


class AutopilotManager:
    """
    Manages long-running external autopilot tasks (takeoff / landing) that run
    in the background and control the aircraft via SimConnect.

    The LLM interacts only via MCP tools that call these methods.
    """

    def __init__(self, msfs_client: MsfsClient):
        self._msfs = msfs_client
        self._tasks: Dict[str, asyncio.Task] = {}
        self._status: Dict[str, AutopilotStatus] = {}

    def _new_id(self) -> str:
        return str(uuid.uuid4())

    def list_ids(self) -> List[str]:
        return list(self._tasks.keys())

    def get_status(self, autopilot_id: str) -> Optional[AutopilotStatus]:
        return self._status.get(autopilot_id)

    def list_statuses(self) -> List[AutopilotStatus]:
        return list(self._status.values())

    def start_takeoff(
        self,
        runway_heading_deg: float,
        vr_knots: float,
        v2_knots: float,
        target_climb_alt_ft: float,
        update_interval: float = 0.05,
    ) -> str:
        autopilot_id = self._new_id()
        status = AutopilotStatus(
            id=autopilot_id,
            mode=AutopilotMode.TAKEOFF,
            active=True,
            phase="initial",
            last_update=time.time(),
        )
        self._status[autopilot_id] = status

        async def _runner():
            try:
                await self._run_takeoff_loop(
                    autopilot_id,
                    runway_heading_deg,
                    vr_knots,
                    v2_knots,
                    target_climb_alt_ft,
                    update_interval,
                )
            except asyncio.CancelledError:
                status.phase = "cancelled"
                raise
            finally:
                status.active = False
                status.last_update = time.time()
                self._tasks.pop(autopilot_id, None)
                if status.phase not in {"cancelled", "stopped"}:
                    status.phase = "finished"

        task = asyncio.create_task(_runner(), name=f"takeoff_ap_{autopilot_id}")
        self._tasks[autopilot_id] = task
        return autopilot_id

    def start_landing(
        self,
        runway_heading_deg: float,
        vref_knots: float,
        glideslope_deg: float = 3.0,
        decision_alt_ft: float = 200.0,
        update_interval: float = 0.05,
    ) -> str:
        autopilot_id = self._new_id()
        status = AutopilotStatus(
            id=autopilot_id,
            mode=AutopilotMode.LANDING,
            active=True,
            phase="initial",
            last_update=time.time(),
        )
        self._status[autopilot_id] = status

        async def _runner():
            try:
                await self._run_landing_loop(
                    autopilot_id,
                    runway_heading_deg,
                    vref_knots,
                    glideslope_deg,
                    decision_alt_ft,
                    update_interval,
                )
            except asyncio.CancelledError:
                status.phase = "cancelled"
                raise
            finally:
                status.active = False
                status.last_update = time.time()
                self._tasks.pop(autopilot_id, None)
                if status.phase not in {"cancelled", "stopped"}:
                    status.phase = "finished"

        task = asyncio.create_task(_runner(), name=f"landing_ap_{autopilot_id}")
        self._tasks[autopilot_id] = task
        return autopilot_id

    async def stop(self, autopilot_id: str) -> bool:
        task = self._tasks.pop(autopilot_id, None)
        status = self._status.get(autopilot_id)
        if task is None:
            return False
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if status is not None:
            status.active = False
            status.phase = "stopped"
            status.last_update = time.time()
        return True

    async def _run_takeoff_loop(
        self,
        autopilot_id: str,
        runway_heading_deg: float,
        vr_knots: float,
        v2_knots: float,
        target_climb_alt_ft: float,
        update_interval: float,
    ):
        """
        Very simple external takeoff autopilot:
        - Applies takeoff power automatically.
        - Controls rudder to keep heading near runway_heading_deg.
        - Rotates at vr_knots, targets V2 on initial climb.
        - Hands off to whoever called it once target_climb_alt_ft is reached.
        """

        status = self._status[autopilot_id]

        async def read(name: str, units: Optional[str] = None):
            return await _call_msfs(self._msfs.get_simvar, name, units)

        ROLL_HEADING_KP = 4000.0  # Unused, overridden in loop
        ROLL_HEADING_KI = 20.0  # Integral gain (Reduced from 50)
        ROLL_HEADING_KD = 200.0 # Derivative gain (Reduced from 400)
        PITCH_SPEED_KP = 15.0    # Even lower gain for gentler pitch control
        PITCH_RATE_DAMPING = 1500.0  # Damping to prevent oscillations
        # Filters/state for climb controller
        filtered_speed_error: float = 0.0
        last_pitch_cmd: float = 0.0
        flaps_retracted: bool = False

        phase = "roll"
        status.phase = phase
        heading_integral = 0.0  # Integral accumulator for heading error
        heading_integral_bank = 0.0 # Integral accumulator for bank target (climb phase)
        last_heading_error = 0.0 # For derivative calculation

        # Record starting altitude and heading
        start_alt_raw = await read("PLANE_ALTITUDE", "Feet")
        current_heading_raw = await read("PLANE_HEADING_DEGREES_MAGNETIC", "Degrees")
        
        if start_alt_raw is None or current_heading_raw is None:
            log.error(f"Takeoff AP {autopilot_id}: Failed to read initial altitude/heading")
            return
            
        # Apply Takeoff Power (100%)
        await _call_msfs(self._msfs.fire_event, "THROTTLE_SET", 16383)
            
        start_alt = float(start_alt_raw)
        # Convert initial heading from Radians to Degrees
        current_heading = float(current_heading_raw) * (180.0 / math.pi)
        current_heading = (current_heading + 360.0) % 360.0
        
        status.extra["start_alt"] = start_alt
        status.extra["start_heading"] = current_heading
        
        # Auto-calculate speeds
        # Priority: DESIGN_SPEED_VS0 for Vr; DESIGN_SPEED_VS1/VS2 for climb target
        vs0_raw = await read("DESIGN_SPEED_VS0", "Knots")
        vs1_raw = await read("DESIGN_SPEED_VS1", "Knots")
        vs2_raw = await read("DESIGN_SPEED_VS2", "Knots")

        calc_vr = None
        calc_vs1 = None
        calc_vs2 = None

        if vs0_raw is not None:
            val = float(vs0_raw)
            if val > 0:
                calc_vr = val

        if vs1_raw is not None:
            val = float(vs1_raw)
            if val > 0:
                calc_vs1 = val

        if vs2_raw is not None:
            val = float(vs2_raw)
            if val > 0:
                calc_vs2 = val

        if calc_vr is not None:
            vr_knots = calc_vr
        else:
            log.warning(f"Takeoff AP {autopilot_id}: Could not determine DESIGN_SPEED_VS0. Using provided Vr={vr_knots}")

        # Derive V2 if missing: prefer VS2; fallback to 1.2*Vr
        if calc_vs2 is not None:
            v2_knots = calc_vs2
        else:
            v2_knots = vr_knots * 1.2

        # Climb target is midpoint between VS1 and VS2
        climb_target_knots = None
        if calc_vs1 is not None and v2_knots is not None:
            climb_target_knots = (calc_vs1 + v2_knots) / 2.0
        elif calc_vs1 is not None:
            climb_target_knots = (calc_vs1 + (vr_knots * 1.2)) / 2.0
        else:
            # Fallback: use 1.1*Vr as approximate climb target if VS1 missing
            climb_target_knots = vr_knots * 1.1

        # Log sources
        src_vs1 = "DESIGN_SPEED_VS1" if calc_vs1 is not None else "fallback"
        src_vs2 = "DESIGN_SPEED_VS2" if calc_vs2 is not None else "Vr*1.2"
        log.info(f"Takeoff AP {autopilot_id}: Auto-set speeds. Vr={vr_knots:.1f} (VS0), VS1={calc_vs1 if calc_vs1 is not None else 'n/a'}, VS2={v2_knots:.1f} ({src_vs2}), ClimbTarget={climb_target_knots:.1f}")
        
        # Lock the lateral target to the requested runway heading.
        # This ensures we maintain 300° (example) instead of drifting to the
        # initial track and later chasing changing targets.
        target_heading = float(runway_heading_deg) % 360.0
        
        # Check if we're reasonably aligned with runway (within 45 degrees)
        # This check might fail if MagVar is large, so we just log it
        heading_diff = (runway_heading_deg - current_heading + 540.0) % 360.0 - 180.0
        last_heading_error = 0.0 # Initialize to prevent derivative spike
        if abs(heading_diff) > 45:
            log.warning(f"Takeoff AP {autopilot_id}: Large heading difference {heading_diff:.0f}° (current_true={current_heading:.0f}°, runway_mag={runway_heading_deg:.0f}°). Using current heading as target.")
        
        # Release parking brake
        await _call_msfs(self._msfs.fire_event, "PARKING_BRAKES", 0)
        await asyncio.sleep(0.1)
        
        # Apply full throttle for takeoff
        await _call_msfs(self._msfs.fire_event, "THROTTLE_FULL", 0)
        log.info(f"Takeoff AP {autopilot_id}: Starting control loop, Vr={vr_knots}, V2={v2_knots}, target_alt={target_climb_alt_ft}, target_hdg={target_heading:.1f}°")

        last_loop_time = time.time()
        while True:
            # Init control variables for logging
            p_term = 0
            i_term = 0
            d_term = 0
            rudder_command_clamped = 0
            rotate_cmd = 0
            aileron_cmd = 0
            pitch_cmd = 0
            heading_error = 0
            
            current_time = time.time()
            dt = current_time - last_loop_time
            if dt < 0.001: dt = 0.001
            last_loop_time = current_time
            
            status.last_update = current_time
            
            # Read simvars with None handling
            on_ground_raw = await read("SIM_ON_GROUND", "Bool")
            ias_raw = await read("AIRSPEED_INDICATED", "Knots")
            heading_mag_raw = await read("PLANE_HEADING_DEGREES_MAGNETIC", "Degrees")
            # Do NOT use GPS track for lateral control; we want to hold heading 300°
            # even in crosswinds. Using track causes the AP to chase wind-caused drift.
            track_mag_raw = None
            ground_speed_raw = await read("GROUND_VELOCITY", "Knots")
            altitude_raw = await read("PLANE_ALTITUDE", "Feet")
            rudder_raw = await read("RUDDER_POSITION")
            bank_raw = await read("PLANE_BANK_DEGREES", "Degrees")
            pitch_raw = await read("PLANE_PITCH_DEGREES", "Degrees")
            pitch_rate_raw = await read("ROTATION_VELOCITY_BODY_Y", "Degrees per second")
            vs_raw = await read("VERTICAL_SPEED", "Feet per second") # Note: SimConnect usually returns ft/min or ft/s depending on unit. Let's ask for ft/min
            beta_raw = await read("INCIDENCE_BETA", "Radians")
            
            # Skip this iteration if any critical value is None
            if any(v is None for v in [on_ground_raw, ias_raw, heading_mag_raw, altitude_raw, bank_raw, pitch_raw]):
                await asyncio.sleep(update_interval)
                continue
            
            on_ground = bool(on_ground_raw)
            ias = float(ias_raw)
            altitude = float(altitude_raw)
            rudder = float(rudder_raw) if rudder_raw is not None else 0.0
            beta = float(beta_raw) if beta_raw is not None else 0.0
            
            # Convert angles from Radians to Degrees if necessary (SimConnect default behavior check)
            # We asked for "Degrees", but let's be safe. If values are small (< 6.3) when they should be large, they are radians.
            # But we already fixed heading by multiplying. Let's do the same for bank/pitch if needed.
            # Actually, let's trust the unit request but verify.
            # For now, assume the previous fix (manual conversion) is what works because the wrapper might ignore units.
            
            heading_mag = float(heading_mag_raw) * (180.0 / math.pi)
            heading_mag = (heading_mag + 360.0) % 360.0
            
            # Invert bank angle: SimConnect/MSFS seems to report Positive for Left Bank in this context (or we need to normalize it).
            # Log analysis showed Positive Bank (+28) while Heading Decreased (Left Turn).
            # So we invert it so Positive = Right Bank.
            # UPDATE: Documentation says PLANE_BANK_DEGREES is Radians.
            # Standard SimConnect convention is Positive = Right Bank.
            # The previous inversion caused positive feedback (crash).
            # We revert to non-inverted, but ensure conversion from Radians to Degrees.
            bank = float(bank_raw) * (180.0 / math.pi)
            
            # SimConnect Pitch: Positive = Nose Down. We want Positive = Nose Up.
            pitch = -1.0 * float(pitch_raw) * (180.0 / math.pi)
            
            # Pitch Rate: Invert so Positive = Nose Up Rate
            pitch_rate_val = float(pitch_rate_raw) if pitch_rate_raw is not None else 0.0
            pitch_rate = -1.0 * pitch_rate_val * (180.0 / math.pi)
            
            # Vertical speed
            vs = float(vs_raw) if vs_raw is not None else 0.0
            # If vs is very small, it might be ft/s. If we want ft/min:
            # Let's assume it returns what we asked. If we asked "Feet per second", it's ft/s.
            # Let's re-request as "Feet per minute" in the next loop iteration if we could, but we can't change it dynamically easily.
            # Let's just use what we have.
            
            ground_speed = float(ground_speed_raw) if ground_speed_raw is not None else 0.0
            
            # Always use magnetic heading for heading hold (not GPS track).
            heading = heading_mag
            hdg_src = "MAG"
            log.debug(f"Takeoff AP loop: phase={phase}, ias={ias:.1f}, alt={altitude:.0f}, hdg={heading:.0f}, bank={bank:.1f}, pitch={pitch:.1f}")
            status.extra.update(
                {
                    "phase": phase,
                    "on_ground": on_ground,
                    "ias": ias,
                    "heading": heading,
                    "altitude": altitude,
                    "heading_source": hdg_src
                }
            )

            if phase == "roll":
                # Check for rotation speed FIRST
                if ias >= vr_knots:
                    phase = "rotate"
                    status.phase = phase
                    log.info(f"Takeoff AP {autopilot_id}: ROTATE BEGIN Vr={vr_knots:.1f} IAS={ias:.1f} (Vr source VS0)")
                    blackbox.log_action({
                        "timestamp": current_time,
                        "phase": phase,
                        "ias": ias,
                        "altitude": altitude,
                        "heading": heading,
                        "pitch": pitch,
                        "on_ground": on_ground,
                    }, "rotate_begin", f"Vr={vr_knots:.1f}")
                
                # Use rudder for ground steering during takeoff roll
                # If we are no longer on the ground, stop steering immediately
                elif not on_ground:
                    await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", 0)
                    # If we are airborne but too slow, just wait.
                    # Do not steer, do not rotate.

                else:
                    heading_error = (target_heading - heading + 540.0) % 360.0 - 180.0
                    
                    # Dynamic gain scaling based on airspeed
                    # Rudder becomes more effective at higher speeds, so we reduce gains
                    speed_scaling = 1.0
                    if ias > 30:
                        speed_scaling = 30.0 / ias
                    
                # PID controller: Proportional + Integral + Derivative
                # Progressive proportional gain
                abs_error = abs(heading_error)
                if abs_error < 2:
                    # Small errors: Strong correction but stable
                    base_kp = 500 # Reduced from 1000
                elif abs_error < 8:
                    # Medium errors: Progressive
                    base_kp = 400 # Reduced from 800
                else:
                    # Large errors: Cap gain
                    base_kp = 300 # Reduced from 600
                
                p_term = int(-heading_error * base_kp * speed_scaling)
                
                # Minimum command boost to overcome friction/deadband (reduce boost at high speed)
                min_cmd = 200 * speed_scaling # Reduced from 500
                if abs_error > 0.1 and abs(p_term) < min_cmd:
                    p_term = int(min_cmd if p_term > 0 else -min_cmd)

                # Integral term accumulates error over time
                heading_integral += heading_error * dt
                i_term = int(-ROLL_HEADING_KI * heading_integral * speed_scaling)
                
                # Derivative term to dampen oscillations
                error_rate = (heading_error - last_heading_error) / dt
                d_term = int(-error_rate * ROLL_HEADING_KD * speed_scaling)
                last_heading_error = heading_error

                # Combine P, I, and D terms (increase authority for roll hold)
                rudder_command = p_term + i_term + d_term
                
                # Clamp to stronger maximum deflection for runway tracking
                MAX_RUDDER = 14000
                rudder_command_clamped = max(-MAX_RUDDER, min(MAX_RUDDER, rudder_command))
                
                # Anti-windup: don't accumulate integral if saturated
                if rudder_command != rudder_command_clamped:
                    heading_integral -= heading_error * dt
                
                await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", rudder_command_clamped)
                blackbox.log_action({
                    "timestamp": current_time,
                    "phase": phase,
                    "ias": ias,
                    "heading": heading,
                    "target_heading": runway_heading_deg,
                    "heading_error": heading_error,
                    "rudder_cmd": rudder_command_clamped,
                    "p_term": p_term,
                    "i_term": i_term,
                    "d_term": d_term,
                    "on_ground": on_ground,
                }, "heading_control_roll", "aggressive_pid")

                # Explicitly hold elevator neutral during roll until VS0 reached
                # Prevent any pitch-up before Vr/VS0
                # Begin gentle 5° pitch-up when IAS >= (VS0 - 5 kt), still capped at 5° while on ground
                if phase == "roll":
                    if on_ground and ias >= (vr_knots - 10.0):
                        target_pitch_hold = 5.0
                        pitch_error_hold = target_pitch_hold - pitch
                        hold_kp = 500
                        hold_kd = 400
                        hold_cmd = int((-pitch_error_hold * hold_kp) + (pitch_rate * hold_kd))
                        # Cap nose-up while on ground to avoid exceeding 5°
                        if pitch >= 5.0 and hold_cmd < 0:
                            # limit further nose-up
                            over_error = pitch - 5.0
                            cap_cmd = int(over_error * 800)
                            hold_cmd = max(hold_cmd, cap_cmd)
                        hold_cmd = max(-12000, min(0, hold_cmd))
                        await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", hold_cmd)
                        blackbox.log_action({
                            "timestamp": current_time,
                            "phase": phase,
                            "ias": ias,
                            "altitude": altitude,
                            "heading": heading,
                            "pitch": pitch,
                            "elevator_cmd": hold_cmd,
                            "on_ground": on_ground,
                        }, "early_pitch_hold", f"threshold=VS0-10; target=5deg")
                    else:
                        await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", 0)
                        blackbox.log_action({
                            "timestamp": current_time,
                            "phase": phase,
                            "ias": ias,
                            "altitude": altitude,
                            "heading": heading,
                            "pitch": pitch,
                            "elevator_cmd": 0,
                            "on_ground": on_ground,
                        }, "elevator_neutral", "roll")
                
                log.info(f"Takeoff AP {autopilot_id}: ROLL hdg={heading:.1f}° target={runway_heading_deg:.0f}° error={heading_error:.1f}° | P={p_term} I={i_term} D={d_term} cmd={rudder_command_clamped} | ias={ias:.1f}kt rudder={rudder:.3f}")
                
            elif phase == "rotate":
                # Dynamic Rotation Logic
                # Pull back until pitch is > 10 degrees or we are climbing well
                
                target_pitch = 12.0
                pitch_error = target_pitch - pitch
                
                # P-controller with Damping for rotation
                # Reduced gain to prevent over-rotation
                kp = 400
                kd = 500 # Damping factor
                
                # We want Pitch Up (Negative Cmd).
                # P-term: -Error * Kp (Negative)
                # D-term: Rate * Kd (Positive, to oppose pitch up)
                
                rotate_cmd = int((-pitch_error * kp) + (pitch_rate * kd))
                
                # Clamp to max pull. Reduced from -16000 to -12000 to be gentler.
                rotate_cmd = max(-12000, min(0, rotate_cmd)) 

                # Pre-airborne pitch cap: limit commanded nose-up if pitch exceeds 5° while on_ground
                if on_ground and pitch >= 5.0:
                    # If already beyond 15°, bias nose-down slightly to bring back under cap
                    over_error = pitch - 5.0
                    cap_cmd = int(over_error * 800)  # positive -> nose down
                    rotate_cmd = max(rotate_cmd, cap_cmd)
                
                await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", rotate_cmd)
                blackbox.log_action({
                    "timestamp": current_time,
                    "phase": phase,
                    "ias": ias,
                    "altitude": altitude,
                    "heading": heading,
                    "pitch": pitch,
                    "pitch_rate": pitch_rate,
                    "elevator_cmd": rotate_cmd,
                    "on_ground": on_ground,
                }, "rotate_control", "")
                log.info(f"Takeoff AP {autopilot_id}: ROTATE Pitch={pitch:.1f}° Rate={pitch_rate:.1f}°/s Cmd={rotate_cmd}")
                
                # Check exit conditions
                # 1. Pitch > 10 degrees
                # 2. Positive vertical speed (climbing)
                # 3. Altitude > Start + 50ft
                
                is_climbing = vs > 5.0 
                
                if pitch > 10.0 or (altitude > start_alt + 50):
                    phase = "climb"
                    status.phase = phase
                    log.info(f"Takeoff AP {autopilot_id}: Rotation complete. Pitch={pitch:.1f}, Alt={altitude:.0f}")

                # Begin soft heading capture immediately once airborne, even during rotate
                if not on_ground:
                    # Wings-level priority right after liftoff to avoid drift
                    status.extra.setdefault("airborne_start_time", current_time)
                    status.extra.setdefault("wings_level_until", status.extra["airborne_start_time"] + 2.0)
                    wings_level_active = current_time <= status.extra["wings_level_until"]

                    # Always chase the fixed runway heading target.
                    desired_heading = target_heading
                    hdg_error = (desired_heading - heading + 540.0) % 360.0 - 180.0

                    if wings_level_active:
                        bank_target = 0.0
                        mode = "wings_level"
                        ROLL_KP = 900
                        bank_kd = 150
                    else:
                        # Aggressive capture after wings-level window, then gentle
                        status.extra.setdefault("gentle_capture", False)
                        aggressive = not status.extra["gentle_capture"]
                        if aggressive:
                            # Linear mapping for better response
                            cap = 25.0
                            mapped = abs(hdg_error) * 1.5
                            # Invert bank target
                            bank_target = -1.0 * max(-cap, min(cap, math.copysign(mapped, hdg_error)))
                            mode = "aggressive"
                            ROLL_KP = 1200
                            bank_kd = 350
                        else:
                            cap = 10.0
                            mapped = abs(hdg_error) * 1.0
                            # Invert bank target
                            bank_target = -1.0 * max(-cap, min(cap, math.copysign(mapped, hdg_error)))
                            mode = "gentle"
                            ROLL_KP = 800
                            bank_kd = 300

                    bank_error = bank_target - bank
                    status.extra.setdefault("last_bank", bank)
                    bank_rate = (bank - status.extra["last_bank"]) / max(dt, 1e-3)
                    status.extra["last_bank"] = bank
                    aileron_cmd = int(bank_error * ROLL_KP - bank_rate * bank_kd)
                    aileron_cmd = max(-10000, min(10000, aileron_cmd))
                    await _call_msfs(self._msfs.fire_event, "AXIS_AILERONS_SET", aileron_cmd)
                    blackbox.log_action({
                        "timestamp": current_time,
                        "phase": phase,
                        "ias": ias,
                        "heading": heading,
                        "target_heading": desired_heading,
                        "heading_error": hdg_error,
                        "bank": bank,
                        "aileron_cmd": aileron_cmd,
                        "on_ground": on_ground,
                    }, "heading_capture_airborne", f"mode={mode}; bank_target={bank_target:.1f}; kp={ROLL_KP}; kd={bank_kd}; bank_rate={bank_rate:.2f}")
                    # Switch to gentle mode after holding alignment for stability
                    status.extra.setdefault("gentle_capture", False)
                    status.extra.setdefault("aligned_since", None)
                    if abs(hdg_error) < 2.0:
                        status.extra["aligned_since"] = status.extra["aligned_since"] or current_time
                        if not status.extra["gentle_capture"] and (current_time - status.extra["aligned_since"]) > 2.5:
                            status.extra["gentle_capture"] = True
                    else:
                        status.extra["aligned_since"] = None

            elif phase == "climb":
                # Check for auto-disengage at target climb altitude
                if altitude > (start_alt + target_climb_alt_ft):
                    log.info(f"Takeoff AP {autopilot_id}: Reached target altitude ({altitude:.0f}ft). Disengaging.")
                    # Center controls before exiting
                    await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", 0)
                    await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", 0)
                    await _call_msfs(self._msfs.fire_event, "AXIS_AILERONS_SET", 0)
                    break

                # Ensure rudder is centered once we are climbing
                await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", 0)

                # --- SOFT HEADING CAPTURE + WINGS LEVEL ---
                # Keep wings near level early in climb to avoid lateral drift, then gentle capture.
                # Always chase the fixed runway heading target.
                desired_heading = target_heading
                hdg_error = (desired_heading - heading + 540.0) % 360.0 - 180.0
                status.extra.setdefault("airborne_start_time", current_time)
                status.extra.setdefault("wings_level_until", status.extra["airborne_start_time"] + 2.0)
                wings_level_active = current_time <= status.extra["wings_level_until"]
                if wings_level_active:
                    bank_target = 0.0
                    ROLL_KP = 1000
                    bank_kd = 150
                    capture_mode = "wings_level"
                else:
                    # Aggressive until aligned, then gentle; exponential mapping + damping
                    status.extra.setdefault("gentle_capture", False)
                    status.extra.setdefault("aligned_since", None)
                    if not status.extra["gentle_capture"]:
                        cap = 25.0
                        mapped = abs(hdg_error) * 1.5
                        # Invert bank target: Positive Error (Right Turn) requires Right Bank (Negative Value)
                        bank_target = -1.0 * max(-cap, min(cap, math.copysign(mapped, hdg_error)))
                        ROLL_KP = 1200
                        bank_kd = 350
                        capture_mode = "aggressive"
                        if abs(hdg_error) < 1.5:
                            status.extra["aligned_since"] = status.extra["aligned_since"] or current_time
                            if (current_time - status.extra["aligned_since"]) > 2.5:
                                status.extra["gentle_capture"] = True
                        else:
                            status.extra["aligned_since"] = None
                    else:
                        cap = 10.0
                        mapped = abs(hdg_error) * 1.0
                        # Invert bank target
                        bank_target = -1.0 * max(-cap, min(cap, math.copysign(mapped, hdg_error)))
                        ROLL_KP = 800
                        bank_kd = 300
                        capture_mode = "gentle"
                
                # Integral term for heading hold (to correct steady-state error from P-factor/Torque)
                if not wings_level_active:
                    heading_integral_bank += hdg_error * dt
                    # Clamp integral to +/- 20 degrees equivalent bank contribution
                    heading_integral_bank = max(-40.0, min(40.0, heading_integral_bank))
                    ROLL_KI_BANK = 0.5
                    # Invert integral application: Positive Error -> Negative Bank Bias
                    bank_target -= heading_integral_bank * ROLL_KI_BANK

                bank_error = bank_target - bank
                status.extra.setdefault("last_bank", bank)
                bank_rate = (bank - status.extra["last_bank"]) / max(dt, 1e-3)
                status.extra["last_bank"] = bank
                aileron_cmd = int(bank_error * ROLL_KP - bank_rate * bank_kd)
                aileron_cmd = max(-10000, min(10000, aileron_cmd))
                
                await _call_msfs(self._msfs.fire_event, "AXIS_AILERONS_SET", aileron_cmd)
                blackbox.log_action({
                    "timestamp": current_time,
                    "phase": phase,
                    "ias": ias,
                    "heading": heading,
                    "target_heading": desired_heading,
                    "heading_error": hdg_error,
                    "bank": bank,
                    "aileron_cmd": aileron_cmd,
                    "on_ground": on_ground,
                }, "heading_capture_airborne", f"mode={capture_mode}; bank_target={bank_target:.1f}; kp={ROLL_KP}; kd={bank_kd}; bank_rate={bank_rate:.2f}")
                # Ensure capture state keys exist to avoid KeyError in rotate logic reuse
                status.extra.setdefault("gentle_capture", False)
                status.extra.setdefault("aligned_since", None)

                # --- PITCH LOGIC ---
                # Phase A: accelerate with a gentle fixed pitch until IAS reaches midpoint target
                # Phase B: once IAS >= midpoint, switch to speed hold using P+D controller
                PITCH_KP = 85
                PITCH_KD = 500

                if ias < climb_target_knots:
                    # Hold 10° nose-up to build speed to target
                    fixed_target_pitch = 10.0
                    pitch_error_hold = fixed_target_pitch - pitch
                    # Nose-up command is negative; add damping on pitch rate
                    pitch_cmd = int((-pitch_error_hold * 500) + (pitch_rate * 400))
                else:
                    # Maintain climb target speed
                    raw_error = climb_target_knots - ias
                    # Wider deadband to reduce hunting
                    if abs(raw_error) < 4.0:
                        raw_error = 0.0
                    # Exponential smoothing on speed error
                    alpha = 0.85
                    filtered_speed_error = (alpha * filtered_speed_error) + ((1 - alpha) * raw_error)
                    pitch_cmd = int((filtered_speed_error * PITCH_KP) + (pitch_rate * PITCH_KD))
                
                # SAFETY: Minimum Pitch Protection during initial climb
                # Prevent diving to chase airspeed if we are below a safe climb angle
                MIN_CLIMB_PITCH = 8.0
                protection_applied = False
                if pitch < MIN_CLIMB_PITCH:
                    # If too low, forbid nose-down commands and bias nose-up
                    pitch_error_protection = MIN_CLIMB_PITCH - pitch
                    protection_cmd = int(-pitch_error_protection * 1200)
                    # If current command is nose-down (positive), override with protection
                    if pitch_cmd > 0:
                        pitch_cmd = protection_cmd
                        protection_applied = True
                    else:
                        # Take the stronger nose-up
                        pitch_cmd = min(pitch_cmd, protection_cmd)
                        protection_applied = True
                
                # Pre-airborne pitch cap: if still flagged on_ground, enforce 5° cap
                if on_ground and pitch >= 5.0:
                    over_error = pitch - 5.0
                    cap_cmd = int(over_error * 800)
                    if pitch_cmd < 0:
                        # limit nose-up so we don't increase beyond cap
                        pitch_cmd = max(pitch_cmd, cap_cmd)
                    else:
                        # allow nose-down to reduce pitch
                        pitch_cmd = max(pitch_cmd, cap_cmd)

                # Slew-rate limit to avoid rapid reversals
                MAX_DELTA_CMD = 700
                delta_cmd = pitch_cmd - int(last_pitch_cmd)
                if delta_cmd > MAX_DELTA_CMD:
                    pitch_cmd = int(last_pitch_cmd) + MAX_DELTA_CMD
                elif delta_cmd < -MAX_DELTA_CMD:
                    pitch_cmd = int(last_pitch_cmd) - MAX_DELTA_CMD
                last_pitch_cmd = pitch_cmd

                # Clamp
                pitch_cmd = max(-10000, min(10000, pitch_cmd))
                
                await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", pitch_cmd)
                blackbox.log_action({
                    "timestamp": current_time,
                    "phase": phase,
                    "ias": ias,
                    "pitch": pitch,
                    "pitch_rate": pitch_rate,
                    "elevator_cmd": pitch_cmd,
                    "on_ground": on_ground,
                }, "climb_pitch_control", f"target={climb_target_knots:.1f}; filtered_err={filtered_speed_error:.2f}")
                log.info(f"Takeoff AP {autopilot_id}: CLIMB IAS={ias:.1f} Target={climb_target_knots:.1f} Pitch={pitch:.1f} Rate={pitch_rate:.1f} Cmd={pitch_cmd} Protect={protection_applied}")

                # Retract flaps once we reach (midpoint - 10 kt) and are airborne
                flaps_retract_speed = max(0.0, climb_target_knots - 10.0)
                if not flaps_retracted and not on_ground and ias >= flaps_retract_speed:
                    try:
                        await _call_msfs(self._msfs.fire_event, "FLAPS_UP", 0)
                        flaps_retracted = True
                        log.info(f"Takeoff AP {autopilot_id}: Flaps retraction triggered at IAS={ias:.1f} (threshold {flaps_retract_speed:.1f})")
                        blackbox.log_action({
                            "timestamp": current_time,
                            "phase": phase,
                            "ias": ias,
                            "altitude": altitude,
                            "heading": heading,
                            "on_ground": on_ground,
                        }, "flaps_up", f"threshold={flaps_retract_speed:.1f}")
                    except Exception as e:
                        log.warning(f"Takeoff AP {autopilot_id}: Failed to retract flaps: {e}")

            # Log data
            log_data = {
                "timestamp": current_time,
                "phase": phase,
                "ias": ias,
                "ground_speed": ground_speed,
                "altitude": altitude,
                "heading": heading,
                "target_heading": target_heading if phase == "roll" else "",
                "heading_error": heading_error if phase == "roll" else "",
                "rudder_cmd": rudder_command_clamped,
                "p_term": p_term,
                "i_term": i_term,
                "d_term": d_term,
                "pitch": pitch,
                "pitch_rate": pitch_rate,
                "elevator_cmd": rotate_cmd if phase == "rotate" else pitch_cmd,
                "bank": bank,
                "aileron_cmd": aileron_cmd,
                "on_ground": on_ground,
                "beta": beta
            }
            blackbox.log(log_data)

            await asyncio.sleep(update_interval)

    async def _run_landing_loop(
        self,
        autopilot_id: str,
        runway_heading_deg: float,
        vref_knots: float,
        glideslope_deg: float,
        decision_alt_ft: float,
        update_interval: float,
    ):
        """
        Simplified landing autopilot:
        - Assumes aircraft is already on or near final.
        - Tracks runway heading and a synthetic glideslope.
        - Controls pitch for glideslope, rudder/aileron for centerline.
        - Performs a simple flare near touchdown.
        """

        status = self._status[autopilot_id]

        async def read(name: str, units: Optional[str] = None):
            return await _call_msfs(self._msfs.get_simvar, name, units)

        HDG_KP = 300.0
        GS_KP = 2000.0
        FLARE_ALT_FT = 20.0

        phase = "approach"
        status.phase = phase

        ref_alt = float(await read("PLANE_ALTITUDE"))
        glideslope_rad = math.radians(glideslope_deg)
        start_time = time.time()

        while True:
            status.last_update = time.time()

            on_ground = bool(await read("SIM_ON_GROUND"))
            ias = float(await read("AIRSPEED_INDICATED", "knots"))
            heading = float(await read("PLANE_HEADING_DEGREES_TRUE"))
            altitude = float(await read("PLANE_ALTITUDE"))
            status.extra.update(
                {
                    "phase": phase,
                    "on_ground": on_ground,
                    "ias": ias,
                    "heading": heading,
                    "altitude": altitude,
                }
            )

            if on_ground and altitude < FLARE_ALT_FT:
                phase = "rollout"
                status.phase = phase

            if phase == "approach":
                heading_error = (runway_heading_deg - heading + 540.0) % 360.0 - 180.0
                rudder_cmd = max(min(HDG_KP * (heading_error / 30.0), 16383), -16383)
                await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", int(rudder_cmd))

                elapsed = max(time.time() - start_time, 0.0)
                groundspeed_fps = max(ias, 1.0) * 1.68781
                distance_along_path = groundspeed_fps * elapsed
                target_alt = ref_alt - math.tan(glideslope_rad) * distance_along_path
                gs_error = target_alt - altitude
                elevator_cmd = max(min(GS_KP * (gs_error / 1000.0), 12000), -12000)
                await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", int(elevator_cmd))

                if altitude <= decision_alt_ft:
                    phase = "flare"
                    status.phase = phase
                    continue

            elif phase == "flare":
                if altitude <= FLARE_ALT_FT:
                    await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", int(2000))
                if on_ground:
                    phase = "rollout"
                    status.phase = phase
                    continue

            elif phase == "rollout":
                heading_error = (runway_heading_deg - heading + 540.0) % 360.0 - 180.0
                rudder_cmd = max(min(HDG_KP * (heading_error / 30.0), 16383), -16383)
                await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", int(rudder_cmd))

                if ias < 30.0:
                    await _call_msfs(self._msfs.fire_event, "AXIS_RUDDER_SET", 0)
                    await _call_msfs(self._msfs.fire_event, "AXIS_ELEVATOR_SET", 0)
                    return

            await asyncio.sleep(update_interval)

# --------- Scripting Support --------- #

# Scripting support removed: ScriptManager and dynamic exec-based control


# --------- FastAPI + MCP app --------- #

msfs = MsfsClient()
autopilot_manager = AutopilotManager(msfs)


async def _call_msfs(func, *args, **kwargs):
    """Run blocking SimConnect operations in a thread."""
    return await asyncio.to_thread(func, *args, **kwargs)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(title="MSFS MCP Server", lifespan=lifespan)
mcp_app = mcp.streamable_http_app()
app.mount("/mcp", mcp_app)


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "MSFS MCP server running. JSON-RPC at /rpc, MCP transport at /mcp/",
    }


@app.get("/healthz")
def health():
    try:
        msfs.connect()
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/rpc", response_model=JsonRpcResponse)
def rpc_endpoint(req: JsonRpcRequest):
    """
    Minimal JSON-RPC 2.0 style endpoint.
    """
    try:
        result = dispatch_method(req.method, req.params or {})
        return JsonRpcResponse(id=req.id, result=result)
    except HTTPException as he:
        return JsonRpcResponse(
            id=req.id,
            error={"code": he.status_code, "message": he.detail},
        )
    except Exception as exc:
        log.exception("Error handling method %s", req.method)
        return JsonRpcResponse(
            id=req.id,
            error={"code": -32000, "message": str(exc)},
        )


def dispatch_method(method: str, params: Dict[str, Any]) -> Any:
    """
    Map JSON-RPC methods to MSFS actions.
    """
    m = method.lower()

    if m == "msfs.get_state":
        # {"as_lines": true} -> newline-separated NAME: value string
        as_lines = bool(params.get("as_lines", False))
        group = params.get("group")
        include_groups = bool(params.get("include_groups", False))
        return msfs.get_state(
            as_lines=as_lines,
            group=group,
            include_groups=include_groups,
        )

    if m == "msfs.list_state_groups":
        return msfs.list_state_groups()

    if m == "msfs.list_event_groups":
        include_events = bool(params.get("include_events", False))
        return msfs.list_event_groups(include_events=include_events)

    if m == "msfs.get_event_group":
        group = params.get("group")
        if not group:
            raise HTTPException(400, "group required")
        try:
            return msfs.get_event_group(group)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    if m == "msfs.set_event_state":
        group = params.get("group")
        event = params.get("event")
        value = params.get("value")
        if not group or not event:
            raise HTTPException(400, "group and event required")
        try:
            return msfs.set_event_state(group, event, value)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    if m == "msfs.set_event_states":
        operations = params.get("operations")
        if operations is None:
            raise HTTPException(400, "operations list required")
        try:
            return msfs.set_event_states(operations)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    raise HTTPException(404, f"Unknown method: {method}")


# --------- MCP tools --------- #


@mcp.tool(description="Apply multiple SimConnect events sequentially")
async def set_event_states(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not operations:
        raise HTTPException(400, "operations list required")
    try:
        return await _call_msfs(msfs.set_event_states, operations)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc




# --------- MCP tools --------- #


@mcp.tool(description="List available SimConnect helper groups")
async def list_simvar_groups() -> List[Dict[str, Any]]:
    return await _call_msfs(msfs.list_state_groups)


@mcp.tool(description="Dump SimVars from a specific helper group. See list_simvar_groups() for available groups. The Strings group contains the plane name and other useful information to get started. IMPORTANT: For aircraft heading, use WISKEY_COMPASS_INDICATION_DEGREES from FlightInstrumentationData group - this is the actual compass heading visible in the cockpit. PLANE_HEADING_DEGREES_TRUE/MAGNETIC from PositionandSpeedData may show incorrect values during ground movement.")
async def get_simvar_group_state(
    group: Optional[str] = None,
    include_groups: bool = False,
    as_lines: bool = False,
):
    return await _call_msfs(msfs.get_state, as_lines, group, include_groups)


@mcp.tool(description="List available SimConnect event helper groups")
async def list_event_groups(include_events: bool = False) -> List[Dict[str, Any]]:
    return await _call_msfs(msfs.list_event_groups, include_events)


@mcp.tool(description="Get all events inside a specific helper group")
async def get_event_group_details(group: str) -> Dict[str, Any]:
    if not group:
        raise HTTPException(400, "group required")
    try:
        return await _call_msfs(msfs.get_event_group, group)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@mcp.tool(description="Send a SimConnect event with an optional value")
async def set_event_state(group: str, event: str, value: Optional[int] = 0) -> Dict[str, Any]:
    if not group or not event:
        raise HTTPException(400, "group and event required")
    try:
        return await _call_msfs(msfs.set_event_state, group, event, value)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


# --------- External autopilot MCP tools --------- #
# Usage guide for LLM agents:
# 1) Use engage_takeoff_autopilot before departure, then monitor status via get_autopilot_status.
#    When the phase transitions to "handoff" (or active becomes False), immediately configure the built-in MSFS AP
#    via set_event_state/set_event_states.
# 2) Use the built-in MSFS autopilot for climb/cruise/descent adjustments; do not drive axes directly outside
#    the takeoff/landing loops.
# 3) Before final approach, disengage pitch/roll AP as needed, then call engage_landing_autopilot when established.
#    Monitor status until it completes (phase "rollout" with low speed) before issuing taxi/cleanup commands.
# 4) If conditions become unsafe, call stop_autopilot to abort and take manual/built-in AP action.


@mcp.tool(
    description=(
        "Engage the external takeoff autopilot. It will keep runway centerline, rotate at Vr, "
        "climb to target altitude, then release controls. Returns an autopilot_id used to query or stop it."
    )
)
async def engage_takeoff_autopilot(
    runway_heading_deg: float,
    vr_knots: float,
    v2_knots: float,
    target_climb_alt_ft: float,
    update_interval: float = 0.05,
) -> str:
    return autopilot_manager.start_takeoff(
        runway_heading_deg=runway_heading_deg,
        vr_knots=vr_knots,
        v2_knots=v2_knots,
        target_climb_alt_ft=target_climb_alt_ft,
        update_interval=update_interval,
    )


@mcp.tool(
    description=(
        "Engage the external landing autopilot. Use when established on or near final. "
        "Tracks runway heading and a glideslope, performs a flare, and rolls out. Returns an autopilot_id."
    )
)
async def engage_landing_autopilot(
    runway_heading_deg: float,
    vref_knots: float,
    glideslope_deg: float = 3.0,
    decision_alt_ft: float = 200.0,
    update_interval: float = 0.05,
) -> str:
    return autopilot_manager.start_landing(
        runway_heading_deg=runway_heading_deg,
        vref_knots=vref_knots,
        glideslope_deg=glideslope_deg,
        decision_alt_ft=decision_alt_ft,
        update_interval=update_interval,
    )


@mcp.tool(description="Stop an external autopilot instance by id.")
async def stop_autopilot(autopilot_id: str) -> bool:
    return await autopilot_manager.stop(autopilot_id)


@mcp.tool(
    description=(
        "Get status for a running or completed external autopilot. "
        "If autopilot_id is omitted, returns all known statuses."
    )
)
async def get_autopilot_status(autopilot_id: Optional[str] = None) -> Any:
    if autopilot_id:
        status = autopilot_manager.get_status(autopilot_id)
        if status is None:
            raise HTTPException(404, f"Unknown autopilot id: {autopilot_id}")
        return status.dict()
    return [s.dict() for s in autopilot_manager.list_statuses()]


if __name__ == "__main__":
    uvicorn.run("msfs_mcp_server:app", host="0.0.0.0", port=3000, reload=False)
