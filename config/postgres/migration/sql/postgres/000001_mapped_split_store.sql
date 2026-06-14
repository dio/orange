CREATE TABLE orange_mapped_split_maps (
    lane text NOT NULL,
    map_version bigint NOT NULL CHECK (map_version > 0),
    map_checksum bytea NOT NULL CHECK (length(map_checksum) = 32),
    map_payload bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (lane, map_version)
);

CREATE TABLE orange_mapped_split_current (
    lane text PRIMARY KEY,
    map_version bigint NOT NULL CHECK (map_version > 0),
    map_checksum bytea NOT NULL CHECK (length(map_checksum) = 32),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (lane, map_version) REFERENCES orange_mapped_split_maps (lane, map_version)
);

CREATE INDEX orange_mapped_split_current_lane_version_idx
    ON orange_mapped_split_current (lane, map_version);

CREATE TABLE orange_mapped_split_resources (
    lane text NOT NULL,
    resource text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    checksum bytea NOT NULL CHECK (length(checksum) = 32),
    envelope_payload bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (lane, resource, version)
);

CREATE INDEX orange_mapped_split_resources_current_idx
    ON orange_mapped_split_resources (lane, resource, version DESC);

CREATE TABLE orange_mapped_split_build_requests (
    lane text PRIMARY KEY,
    dirty boolean NOT NULL DEFAULT true,
    requested_by text NOT NULL DEFAULT '',
    source_revision text NOT NULL DEFAULT '',
    change_hint text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX orange_mapped_split_build_requests_dirty_idx
    ON orange_mapped_split_build_requests (dirty, updated_at);

CREATE TABLE orange_mapped_split_build_leases (
    lane text PRIMARY KEY,
    holder_id text NOT NULL,
    lease_version bigint NOT NULL CHECK (lease_version > 0),
    locked_until timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    generation_started_at timestamptz NOT NULL
);

CREATE INDEX orange_mapped_split_build_leases_acquire_idx
    ON orange_mapped_split_build_leases (lane, locked_until);
