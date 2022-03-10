import {
  BadRequestException,
  createParamDecorator,
  ExecutionContext,
} from '@nestjs/common';
import { Request } from 'express';
import { pickBy, Dictionary, isString, map, forEach, isEmpty } from 'lodash';
import 'dotenv/config';
import { PrismaClient } from '@prisma/client';
import {
  PrismaClientKnownRequestError,
  PrismaClientValidationError,
} from '@prisma/client/runtime';

export interface PaginateParams {
  skip: number;
  take: number;
  page: number;
  limit: number;
  filter?: { [x: string]: string[] }[];
  orderBy?: string[];
}

export type PaginateOptions<M> = {
  prisma: PrismaClient;
  model: string;
  orderByFields?: Column<M>[];
  filterFields?: Column<M>[];
};

type Column<M> = Extract<keyof M, string>;
type Enumerable<T> = T | Array<T>;

enum OrderByOperators {
  ASC = 'asc',
  DESC = 'desc',
}

enum FilterOperators {
  EQUALS = 'equals',
  NOT = 'not',
  IN = 'in',
  NOT_IN = 'notIn',
  LT = 'lt',
  LTE = 'lte',
  GT = 'gt',
  GTE = 'gte',
  CONTAINS = 'contains',
  SEARCH = 'search',
  STARTS_WITH = 'startsWith',
  ENDS_WITH = 'endsWith',
}

export const Paginate = createParamDecorator(
  (_, ctx: ExecutionContext): PaginateParams => {
    const request: Request = ctx.switchToHttp().getRequest();
    const { query } = request;
    const defaultPageSize = parseInt(process.env.DEFAULT_PAGE_SIZE);

    const take = query.limit
      ? parseInt(query.limit.toString(), 10)
      : defaultPageSize;
    const page = query.page ? parseInt(query.page.toString(), 10) : 1;
    const skip = (page - 1) * take;

    // Sanitize filter
    const filter = map(
      pickBy(
        query,
        (param, name) =>
          name.includes('filter.') &&
          (isString(param) ||
            (Array.isArray(param) &&
              (param as any[]).every((p) => isString(p)))),
      ) as Dictionary<string[]>,
      (value, key) => {
        return {
          [key.replace('filter.', '')]: !Array.isArray(value) ? [value] : value,
        };
      },
    );

    // Sanitize orderBy
    const orderBy: string[] = [];
    const params = query.orderBy
      ? !Array.isArray(query.orderBy)
        ? [query.orderBy]
        : query.orderBy
      : undefined;
    if (params) {
      for (const param of params) {
        orderBy.push(param as string);
      }
    }

    return {
      skip,
      take,
      page: query.page ? parseInt(query.page.toString(), 10) : 1,
      limit: query.limit
        ? parseInt(query.limit.toString(), 10)
        : defaultPageSize,
      orderBy,
      filter,
    };
  },
);

export const paginate = async <M>(
  params: PaginateParams,
  options: PaginateOptions<M>,
) => {
  const { prisma, model, orderByFields, filterFields } = options;
  const isEntityKey = (
    entityColumns: Column<M>[],
    column: string,
  ): column is Column<M> => !!entityColumns.find((c) => c === column);

  // Create where clause
  const whereClause = { AND: [] };
  const whereMeta: { [x: string]: string[] }[] = [];
  forEach(params.filter, (field) => {
    map(field, (value, key) => {
      if (isEntityKey(filterFields, key)) {
        const clause: { [x: string]: { [x: string]: any } } = { AND: [] };
        const filteredFieldValue: string[] = [];
        forEach(value, (_value) => {
          const [op, c] = _value.split(':');
          let condition: any;

          try {
            condition = JSON.parse(c);
          } catch (_) {
            condition = c;
          }
          if (
            Object.values(FilterOperators).includes(
              op.toLowerCase() as FilterOperators,
            )
          ) {
            clause.AND.push({ [key]: { [op]: condition } });
            filteredFieldValue.push(_value);
          }
        });
        if (!isEmpty(filteredFieldValue)) {
          whereMeta.push({ [key]: filteredFieldValue });
        }
        whereClause.AND.push(clause);
      }
    });
  });

  // Create orderBy clause
  const orderByClause: Enumerable<{ [x: string]: any }> = [];
  const orderByMeta: string[] = [];
  forEach(params.orderBy, (orderByItem: string) => {
    const [field, op] = orderByItem.split(':');
    if (
      isEntityKey(orderByFields, field) &&
      Object.values(OrderByOperators).includes(
        op.toLowerCase() as OrderByOperators,
      )
    ) {
      orderByClause.push({ [field]: op.toLowerCase() });
      orderByMeta.push(orderByItem);
    }
  });

  // Query from database
  const query = {
    where: whereClause,
    orderBy: orderByClause,
  };
  try {
    const [totalCount, records] = await prisma.$transaction([
      prisma[model].count(query),
      prisma[model].findMany({
        ...query,
        take: params.take,
        skip: params.skip,
      }),
    ]);

    return {
      records,
      meta: {
        totalCount,
        page: params.page,
        limit: params.limit,
        lastPage: Math.ceil(totalCount / params.take),
        orderBy: orderByMeta,
        filter: whereMeta,
      },
    };
  } catch (err) {
    if (
      err instanceof PrismaClientValidationError ||
      err instanceof PrismaClientKnownRequestError
    ) {
      throw new BadRequestException('malformed query');
    }

    throw err;
  }
};
