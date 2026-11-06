package com.electronik54.consumer.downstream;

import com.electronik54.consumer.dto.OrderEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class PartnerNotifier {

    public final AtomicBoolean down = new AtomicBoolean(false);

    public void notifyPartner(OrderEvent e) {
        if (down.get()) {
            log.error("Downstream service down | orderId:{}", e.orderId());
            throw new DownstreamUnavailableException("{{SIMULATED}} Downstream service set to " + String.valueOf(down.get()).toUpperCase());
        }
        log.info("Partner notified for order:{}", e.orderId());
    }

    public boolean partnerStatus() {
        return down.get();
    }

    public boolean partnerStatus(boolean s) {
        down.set(s);
        boolean status = partnerStatus();
        log.warn("Downstream service set to:{}",status);
        return status;
    }
}