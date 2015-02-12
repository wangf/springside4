/*******************************************************************************
 * Copyright (c) 2005, 2014 springside.github.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package org.springside.modules.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.jpa.domain.Specification;
import org.springside.modules.persistence.SearchFilter.Operator;
import org.springside.modules.utils.Collections3;

import com.google.common.collect.Lists;

public class DynamicSpecifications {

	public static <T> Specification<T> bySearchFilter(final Collection<SearchFilter> filters, final Class<T> entityClazz) {
		return new Specification<T>() {
			@Override
			public Predicate toPredicate(Root<T> root, CriteriaQuery<?> query, CriteriaBuilder builder) {
				if (Collections3.isNotEmpty(filters)) {

					List<Predicate> predicates = Lists.newArrayList();
					for (SearchFilter filter : filters) {
						// nested path translate, 如Task的名为"user.name"的filedName, 转换为Task.user.name属性
						String[] names = StringUtils.split(filter.fieldName, ".");
						Path expression = root.get(names[0]);
						for (int i = 1; i < names.length; i++) {
							expression = expression.get(names[i]);
						}

						// logic operator
						switch (filter.operator) {
						case EQ:
							predicates.add(builder.equal(expression, filter.value));
							break;
						case LIKE:
							predicates.add(builder.like(expression, "%" + filter.value + "%"));
							break;
						case GT:
							predicates.add(builder.greaterThan(expression, (Comparable) filter.value));
							break;
						case LT:
							predicates.add(builder.lessThan(expression, (Comparable) filter.value));
							break;
						case GTE:
							predicates.add(builder.greaterThanOrEqualTo(expression, (Comparable) filter.value));
							break;
						case LTE:
							predicates.add(builder.lessThanOrEqualTo(expression, (Comparable) filter.value));
							break;
						case MMAFT:
							predicates.add(builder.greaterThan((Path<Date>)expression, new Date(Long.valueOf((String) filter.value))));
							break;
						case MMBFR:
							predicates.add(builder.lessThan((Path<Date>)expression, new Date(Long.valueOf((String) filter.value))));
							break;
						case AFT:
							try {
								predicates.add(builder.greaterThan((Path<Date>)expression, new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse((String) filter.value)));
							} catch (ParseException e) {
								// calvin, 这里咋处理？我认为，加不上这个条件就拉倒了，
							}
							break;
						case BFR:
							try {
								predicates.add(builder.lessThan((Path<Date>)expression,new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse((String) filter.value)));
							} catch (ParseException e) {
								// calvin, 这里咋处理？我认为，加不上这个条件就拉倒了，
							}
							break;
						}
					}

					// 将所有条件用 and 联合起来
					if (!predicates.isEmpty()) {
						return builder.and(predicates.toArray(new Predicate[predicates.size()]));
					}
				}

				return builder.conjunction();
			}
		};
	}
}
