#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import copy
import json
import os
import random


DEFAULT_INPUT = "/home/cht/datasets/VitaBench/delivery/tasks.json"
DEFAULT_OUTPUT = "/home/cht/datasets/VitaBench/delivery_aug_branch16/tasks.json"
HARD_NEGATIVE_NAME_SUFFIXES = [
    "[near-match-a]",
    "[near-match-b]",
    "[near-match-c]",
    "[near-match-d]",
]
PRICE_OFFSETS = [0.92, 0.96, 1.04, 1.08]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Expand VitaBench delivery tasks with realistic hard-negative stores so that "
            "top-k branch experiments stress transaction models rather than prompt templates."
        )
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to official VitaBench delivery tasks.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to write the expanded tasks.json")
    parser.add_argument(
        "--min-total-stores",
        type=int,
        default=16,
        help="Ensure each task has at least this many stores after augmentation",
    )
    parser.add_argument(
        "--hard-negative-ratio",
        type=float,
        default=0.75,
        help="Fraction of added stores that should be generated as near-match hard negatives",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260520,
        help="Random seed used when sampling donor stores",
    )
    parser.add_argument(
        "--name-suffix",
        default="[aug-distractor]",
        help="Suffix appended to generated distractor store names",
    )
    return parser.parse_args()


def read_json(path):
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def write_json(path, data):
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def required_order(task):
    states = task.get("evaluation_criteria", {}).get("expected_states", [])
    if not states:
        return {}
    orders = states[0].get("required_orders", [])
    return orders[0] if orders else {}


def expected_store_id(task):
    return str(required_order(task).get("store_id", ""))


def expected_product_id(task):
    products = required_order(task).get("products", []) or []
    if not products:
        return ""
    return str(products[0].get("product_id", ""))


def expected_product_detail(task):
    store_id = expected_store_id(task)
    product_id = expected_product_id(task)
    store = task.get("environment", {}).get("stores", {}).get(store_id, {}) or {}
    for product in store.get("products", []) or []:
        if str(product.get("product_id", "")) == product_id:
            return dict(product)
    return {}


def build_donor_pool(tasks):
    pool = []
    for task in tasks:
        stores = task.get("environment", {}).get("stores", {}) or {}
        for store_id, store in stores.items():
            if not isinstance(store, dict):
                continue
            products = store.get("products", []) or []
            if not products:
                continue
            pool.append(
                {
                    "source_task_id": str(task.get("id", "")),
                    "source_store_id": str(store_id),
                    "store": copy.deepcopy(store),
                }
            )
    return pool


def make_aug_store_id(task_id, idx):
    return "AUG_{task}_{idx:03d}".format(task=task_id, idx=idx)


def _retarget_products(store, new_store_id):
    products = []
    for prod_idx, product in enumerate(store.get("products", []) or []):
        copied = copy.deepcopy(product)
        copied["store_id"] = new_store_id
        copied["store_name"] = store.get("name", "")
        copied["product_id"] = "{}_P{:03d}".format(new_store_id, prod_idx)
        products.append(copied)
    store["products"] = products


def retarget_store(store, task_id, idx, suffix):
    new_store_id = make_aug_store_id(task_id, idx)
    cloned = copy.deepcopy(store)
    base_name = str(cloned.get("name", "Augmented Store")).strip() or "Augmented Store"
    cloned["store_id"] = new_store_id
    cloned["name"] = "{} {}".format(base_name, suffix).strip()
    tags = list(cloned.get("tags", []) or [])
    if "aug_distractor" not in tags:
        tags.append("aug_distractor")
    cloned["tags"] = tags
    _retarget_products(cloned, new_store_id)
    return cloned


def _mutate_price(value, ratio):
    try:
        return round(float(value) * ratio, 2)
    except Exception:
        return value


def _mutate_tags(tags, variant_idx):
    tags = list(tags or [])
    if tags:
        tags = tags[:]
        if variant_idx % 2 == 0 and len(tags) > 1:
            tags = tags[:-1]
    marker = "near_match_{}".format(variant_idx % 4)
    if marker not in tags:
        tags.append(marker)
    return tags


def _build_hard_negative_store(task, task_id, idx, rng):
    store_id = expected_store_id(task)
    store = task.get("environment", {}).get("stores", {}).get(store_id, {}) or {}
    target_product = expected_product_detail(task)
    if not store or not target_product:
        return None

    variant_idx = idx % max(1, len(HARD_NEGATIVE_NAME_SUFFIXES))
    new_store_id = make_aug_store_id(task_id, idx)
    cloned = copy.deepcopy(store)
    cloned["store_id"] = new_store_id
    cloned["name"] = "{} {}".format(
        str(cloned.get("name", "Augmented Store")).strip() or "Augmented Store",
        HARD_NEGATIVE_NAME_SUFFIXES[variant_idx],
    ).strip()
    cloned["tags"] = _mutate_tags(cloned.get("tags", []) or [], variant_idx)

    new_products = []
    for prod_idx, product in enumerate(cloned.get("products", []) or []):
        copied = copy.deepcopy(product)
        copied["store_id"] = new_store_id
        copied["store_name"] = cloned["name"]
        copied["product_id"] = "{}_P{:03d}".format(new_store_id, prod_idx)
        if str(product.get("product_id", "")) == str(target_product.get("product_id", "")):
            ratio = PRICE_OFFSETS[variant_idx % len(PRICE_OFFSETS)]
            copied["price"] = _mutate_price(product.get("price"), ratio)
            copied["name"] = "{} {}".format(
                str(product.get("name", "")).strip(),
                HARD_NEGATIVE_NAME_SUFFIXES[variant_idx],
            ).strip()
            copied["tags"] = _mutate_tags(product.get("tags", []) or [], variant_idx)
            if copied.get("attributes") in (None, "", []):
                copied["attributes"] = ["variant_{}".format(variant_idx)]
        new_products.append(copied)

    rng.shuffle(new_products)
    cloned["products"] = new_products
    return cloned


def expand_tasks(tasks, donor_pool, min_total_stores, hard_negative_ratio, seed, suffix):
    rng = random.Random(seed)
    augmented = []
    stats = {
        "tasks_total": len(tasks),
        "tasks_augmented": 0,
        "stores_before_total": 0,
        "stores_after_total": 0,
        "max_stores_before": 0,
        "max_stores_after": 0,
        "added_distractors_total": 0,
        "hard_negative_total": 0,
        "donor_distractor_total": 0,
    }

    for task in tasks:
        task_copy = copy.deepcopy(task)
        stores = task_copy.setdefault("environment", {}).setdefault("stores", {})
        if not isinstance(stores, dict):
            raise ValueError("task {} has non-dict environment.stores".format(task_copy.get("id")))

        original_count = len(stores)
        stats["stores_before_total"] += original_count
        stats["max_stores_before"] = max(stats["max_stores_before"], original_count)

        need = max(0, min_total_stores - original_count)
        if need > 0:
            stats["tasks_augmented"] += 1

        task_id = str(task_copy.get("id", "task"))
        used_store_ids = set(stores.keys())
        hard_negative_budget = int(round(float(need) * max(0.0, min(1.0, hard_negative_ratio))))
        hard_negative_added = 0
        donor_added = 0

        for local_idx in range(hard_negative_budget):
            new_store = _build_hard_negative_store(task_copy, task_id, local_idx, rng)
            if not new_store:
                break
            if new_store["store_id"] in used_store_ids:
                continue
            stores[new_store["store_id"]] = new_store
            used_store_ids.add(new_store["store_id"])
            hard_negative_added += 1
            if len(stores) >= min_total_stores:
                break

        donor_candidates = donor_pool[:]
        rng.shuffle(donor_candidates)
        donor_idx = 0
        while len(stores) < min_total_stores:
            if donor_idx >= len(donor_candidates):
                donor_idx = 0
                rng.shuffle(donor_candidates)
            donor = donor_candidates[donor_idx]
            donor_idx += 1

            new_store = retarget_store(donor["store"], task_id, hard_negative_added + donor_added, suffix)
            new_store_id = new_store["store_id"]
            if new_store_id in used_store_ids:
                continue
            stores[new_store_id] = new_store
            used_store_ids.add(new_store_id)
            donor_added += 1

        added = hard_negative_added + donor_added
        stats["added_distractors_total"] += added
        stats["hard_negative_total"] += hard_negative_added
        stats["donor_distractor_total"] += donor_added

        final_count = len(stores)
        stats["stores_after_total"] += final_count
        stats["max_stores_after"] = max(stats["max_stores_after"], final_count)

        task_copy["_augmentation"] = {
            "kind": "delivery_branch_expansion_hard_negative",
            "min_total_stores": min_total_stores,
            "added_distractor_count": added,
            "hard_negative_count": hard_negative_added,
            "donor_distractor_count": donor_added,
            "expected_store_id": expected_store_id(task_copy),
            "expected_product_id": expected_product_id(task_copy),
        }
        augmented.append(task_copy)

    return augmented, stats


def print_stats(stats):
    total = max(1, stats["tasks_total"])
    print("tasks_total={}".format(stats["tasks_total"]))
    print("tasks_augmented={}".format(stats["tasks_augmented"]))
    print("added_distractors_total={}".format(stats["added_distractors_total"]))
    print("hard_negative_total={}".format(stats["hard_negative_total"]))
    print("donor_distractor_total={}".format(stats["donor_distractor_total"]))
    print(
        "stores_before_avg={:.2f} stores_after_avg={:.2f}".format(
            float(stats["stores_before_total"]) / total,
            float(stats["stores_after_total"]) / total,
        )
    )
    print(
        "stores_before_max={} stores_after_max={}".format(
            stats["max_stores_before"],
            stats["max_stores_after"],
        )
    )


def main():
    args = parse_args()
    tasks = read_json(args.input)
    if not isinstance(tasks, list):
        raise SystemExit("input must be a JSON array of tasks")

    donor_pool = build_donor_pool(tasks)
    if not donor_pool:
        raise SystemExit("no donor stores with products found in input dataset")

    expanded, stats = expand_tasks(
        tasks=tasks,
        donor_pool=donor_pool,
        min_total_stores=max(1, args.min_total_stores),
        hard_negative_ratio=args.hard_negative_ratio,
        seed=args.seed,
        suffix=args.name_suffix,
    )
    write_json(args.output, expanded)

    print("input={}".format(os.path.abspath(args.input)))
    print("output={}".format(os.path.abspath(args.output)))
    print_stats(stats)


if __name__ == "__main__":
    main()
