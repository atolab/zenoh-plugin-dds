//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use crate::vec_into_raw_parts;
use cdr::{CdrLe, Infinite};
use cyclors::*;
use serde_derive::Serialize;
use std::ffi::CString;

const QOS_EVENT_TYPE_NAME: &str = "far_dds_bridge_msgs::msg::QosEvent";
const QOS_EVENT_TOPIC_NAME: &str = "rt/qos_event";

pub(crate) const QOS_EVENT_ALIVE: u32 = 0;
pub(crate) const QOS_EVENT_NOT_ALIVE: u32 = 1;
// pub(crate) const QOS_EVENT_DELAY: u32 = 2;

const DDS_INFINITY: dds_duration_t = dds_duration_t::MAX;

pub(crate) fn create_qos_event_writer(dp: dds_entity_t) -> dds_entity_t {
    let cton = CString::new(QOS_EVENT_TOPIC_NAME.to_string())
        .unwrap()
        .into_raw();
    let ctyn = CString::new(QOS_EVENT_TYPE_NAME.to_string())
        .unwrap()
        .into_raw();

    unsafe {
        let qos = dds_create_qos();
        dds_qset_reliability(
            qos,
            dds_reliability_kind_DDS_RELIABILITY_RELIABLE,
            DDS_INFINITY,
        );
        dds_qset_durability(qos, dds_durability_kind_DDS_DURABILITY_VOLATILE);
        dds_qset_history(qos, dds_history_kind_DDS_HISTORY_KEEP_LAST, 1);

        let t = cdds_create_blob_topic(dp, cton, ctyn, true);
        dds_create_writer(dp, t, qos, std::ptr::null_mut())
    }
}

pub(crate) fn publish_qos_event(
    dp: dds_entity_t,
    dw: dds_entity_t,
    topic: &str,
    robot_id: &str,
    qos_event: u32,
) {
    #[derive(Serialize, PartialEq)]
    struct QosEvent<'a, 'b> {
        topic: &'a str,
        robot_id: &'b str,
        qos_event: u32,
    }
    let ev = QosEvent {
        topic,
        robot_id,
        qos_event,
    };
    let payload = cdr::serialize::<_, _, CdrLe>(&ev, Infinite).unwrap();

    unsafe {
        let bs = payload.to_vec();
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(bs);
        let ctyn = CString::new(QOS_EVENT_TYPE_NAME.to_string())
            .unwrap()
            .into_raw();
        let st = cdds_create_blob_sertype(dp, ctyn as *mut std::os::raw::c_char, true);
        drop(CString::from_raw(ctyn));
        let fwdp = cdds_ddsi_payload_create(st, ddsi_serdata_kind_SDK_DATA, ptr, len as u64);
        dds_writecdr(dw, fwdp as *mut ddsi_serdata);
        drop(Vec::from_raw_parts(ptr, len, capacity));
        cdds_sertype_unref(st);
    }
}
