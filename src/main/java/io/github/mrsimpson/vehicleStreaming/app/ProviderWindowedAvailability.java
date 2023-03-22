package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.FormattablePojo;

import java.time.Instant;

class ProviderWindowedAvailability implements FormattablePojo {
    public String provider;
    public Long totalAvailabilityMilliseconds;
    public Double totalAvailabilityRatio;
    public Instant startOfWindow;
    public Instant endOfWindow;

    public ProviderWindowedAvailability(String provider, Long totalAvailabilityMilliseconds, Double totalAvailabilityRatio, Instant startOfWindow, Instant endOfWindow) {
        this.provider = provider;
        this.totalAvailabilityMilliseconds = totalAvailabilityMilliseconds;
        this.totalAvailabilityRatio = totalAvailabilityRatio;
        this.startOfWindow = startOfWindow;
        this.endOfWindow = endOfWindow;
    }

    @Override
    public String toFormattedString() {
        return "During " + this.startOfWindow + " and " + this.endOfWindow + " " + this.provider + " provided " + this.totalAvailabilityRatio;
    }
}