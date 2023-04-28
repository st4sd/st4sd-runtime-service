# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import flask_restx.reqparse
import pydantic.error_wrappers

import apis.kernel.experiments
import apis.models.errors


def parser_formatting_dsl() -> flask_restx.reqparse.RequestParser:
    arg_parser = flask_restx.reqparse.RequestParser()
    arg_parser.add_argument(
        "outputFormat",
        choices=["json", "yaml"],
        default="json",
        help='Output format',
        location='args')

    return arg_parser


def parser_formatting_parameterised_package() -> flask_restx.reqparse.RequestParser:
    arg_parser = flask_restx.reqparse.RequestParser()
    arg_parser.add_argument(
        "outputFormat",
        choices=["json", "python", "python-pretty"],
        default="json",
        help='Output format',
        location='args')
    arg_parser.add_argument("hideMetadataRegistry", choices=['y', 'n'], default='n', location="args",
                            help="Whether to hide hte registry metadata or not")

    arg_parser.add_argument("hideNone", choices=['y', 'n'], default='y', location="args",
                            help="Whether to hide fields whose value is None")

    arg_parser.add_argument("hideBeta", choices=['y', 'n'], default='y', location="args",
                            help="Whether to hide Beta fields")

    return arg_parser


def parser_to_format_options(parser: flask_restx.reqparse.RequestParser) -> apis.kernel.experiments.FormatOptions:
    try:
        args = parser.parse_args()

        return apis.kernel.experiments.FormatOptions(
            outputFormat=args.outputFormat, hideBeta=args.hideBeta, hideNone=args.hideNone,
            hideMetadataRegistry=args.hideMetadataRegistry
        )
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.ApiError(f"Invalid document formatting arguments - problems: {e.json(indent=2)}")
    except Exception:
        raise apis.models.errors.ApiError(f"Invalid document formatting arguments")
