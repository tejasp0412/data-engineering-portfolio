/*
Input: Takes customer_unique_id.
Action: Runs an MD5 hash function on it.
Output: A unique 32-character string like a1b2c3d4e5f6...

Why do we need this:
1. Anonymity: Hides the original customer_unique_id, which is good for data privacy.
2. Uniqueness: Guarantees a unique key for every customer, even if the source data has duplicates.
3. Consistency: The same customer will always get the same key, every time the model runs.
4. Performance: Joins on a single, fixed-width MD5 hash are often faster than on long, variable-length string IDs.
5. No dependencies: The key is generated from the data itself so there is no need for auto-incrementing sequences which is hard to manage.
*/

{% macro generate_surrogate_key(field_list) %}
    {% set fields = [] %}
    {% for field in field_list %}
        {% do fields.append("coalesce(cast(" ~ field ~ " as varchar), '_null_')") %}
    {% endfor %}
    md5({{ fields | join(" || '-' || ") }})
{% endmacro %}