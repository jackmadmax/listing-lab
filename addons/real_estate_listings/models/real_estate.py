import json
import logging
import os
from urllib.parse import urlparse

import pika
from odoo import models, fields, api
from odoo.exceptions import UserError
from openai import OpenAI

_logger = logging.getLogger(__name__)


class RealEstate(models.Model):
    _name = 'real_estate.listing'
    _description = 'Real Estate Listings'
    _rec_name = 'address'
    _inherit = [
        'mail.thread',
        'mail.activity.mixin'
    ]

    _order = 'is_favorite desc, create_date desc'

    # Basic Information
    property_id = fields.Char(
        string='Property ID',
        help='Unique identifier for the property'
    )

    listing_id = fields.Char(
        string='Listing ID',
        help='Listing identifier'
    )

    mls = fields.Char(
        string='MLS',
        help='Multiple Listing Service identifier'
    )

    mls_id = fields.Char(
        string='MLS ID',
        help='MLS listing identifier'
    )

    mls_status = fields.Char(
        string='MLS Status',
        help='Status in the MLS system'
    )

    mls_status_raw = fields.Char(
        string='MLS Status Raw',
        help='Raw MLS status string from the source'
    )

    permalink = fields.Char(
        string='Permalink',
        help='Permanent link to the property'
    )

    # Currency
    currency_id = fields.Many2one(
        'res.currency',
        string='Currency',
        default=lambda self: self.env.company.currency_id,
        help='Currency for monetary values on this listing'
    )

    # Price Information
    price = fields.Monetary(
        string='List Price',
        currency_field='currency_id',
        help='Asking price of the property',
        tracking=True,
    )

    list_price_min = fields.Monetary(
        string='Min List Price',
        currency_field='currency_id',
        help='Minimum listing price'
    )

    list_price_max = fields.Monetary(
        string='Max List Price',
        currency_field='currency_id',
        help='Maximum listing price'
    )

    sold_price = fields.Monetary(
        string='Sold Price',
        currency_field='currency_id',
        help='Price the property sold for',
        tracking=True,
    )

    last_sold_price = fields.Monetary(
        string='Last Sold Price',
        currency_field='currency_id',
        help='Previous sold price',
        tracking=True,
    )

    estimated_value = fields.Monetary(
        string='Estimated Value',
        currency_field='currency_id',
        related='best_estimate_id.estimate',
        store=True,
        readonly=True,
        tracking=True,
        help='Estimated property value from the best estimate'
    )

    # Property Description
    property_type = fields.Selection([
        ('single_family', 'Single Family'),
        ('multi_family', 'Multi Family'),
        ('condos', 'Condos'),
        ('condo_townhome', 'Condo/Townhome'),
        ('townhomes', 'Townhomes'),
        ('duplex_triplex', 'Duplex/Triplex'),
        ('farm', 'Farm'),
        ('land', 'Land'),
        ('mobile', 'Mobile')
    ], string='Property Type', help='Type of property')

    # property_type_raw removed

    listing_description = fields.Text(
        string='Description',
        help='Full description of the property'
    )

    description_title = fields.Char(
        string='Description Title',
        help='Title or name from the property description'
    )

    # Address Components
    street = fields.Char(
        string='Street',
        help='Street address'
    )

    street_number = fields.Char(
        string='Street Number',
        help='Street number (e.g., 4832)'
    )

    street_direction = fields.Char(
        string='Street Direction',
        help='Street direction (e.g., N, S, E, W)'
    )

    street_name = fields.Char(
        string='Street Name',
        help='Street name (e.g., Dickenson)'
    )

    street_suffix = fields.Char(
        string='Street Suffix',
        help='Street suffix (e.g., Rd, St, Ave)'
    )

    address_full_line = fields.Char(
        string='Address Full Line',
        help='Full address line (e.g., 4832 N Dickenson Rd)'
    )

    unit = fields.Char(
        string='Unit',
        help='Unit or apartment number'
    )

    city = fields.Char(
        string='City',
        help='City'
    )

    state = fields.Char(
        string='State',
        help='State'
    )

    zip_code = fields.Char(
        string='ZIP Code',
        help='ZIP code'
    )

    county = fields.Char(
        string='County',
        help='County'
    )

    neighborhoods = fields.Char(
        string='Neighborhoods',
        help='Neighborhoods information (JSON format)'
    )

    # Bedrooms and bathrooms
    bedrooms = fields.Integer(
        string='Beds',
        help='Number of bedrooms'
    )

    baths_full = fields.Integer(
        string='Full Baths',
        help='Number of full bathrooms'
    )

    baths_half = fields.Integer(
        string='Half Baths',
        help='Number of half bathrooms'
    )

    baths_total = fields.Float(
        string='Total Baths',
        digits=(5, 1),
        compute='_compute_baths_total',
        inverse='_inverse_baths_total',
        store=True,
        help='Total number of bathrooms'
    )

    bed_bath_description = fields.Char(
        string='Bed/Bath Description',
        help='Bed/bath description',
        compute='_compute_bed_bath_description',
        store=True
    )

    # Basement (keeping existing field)
    basement = fields.Selection([
        ('yes', 'Yes'),
        ('no', 'No'),
        ('finished', 'Finished'),
        ('unfinished', 'Unfinished'),
        ('finished_walkout', 'Finished, walk out')
    ], string='Basement', help='Basement information')

    # Fixtures (keeping existing field)
    fixtures = fields.Char(
        string='Fixtures',
        help='Included fixtures and appliances'
    )

    # Size information
    sqft = fields.Integer(
        string='Square Feet',
        help='Property size in square feet'
    )

    lot_sqft = fields.Integer(
        string='Lot Sqft',
        help='Lot size in square feet'
    )

    # Removed free-text lot summary field per spec

    lot_acres = fields.Float(
        string='Lot Size (Acres)',
        digits=(10, 2),
        compute='_compute_lot_acres',
        inverse='_inverse_lot_acres',
        store=True,
        help='Lot size in acres; kept in sync with Lot Sqft'
    )

    stories = fields.Float(
        string='Stories',
        help='Number of stories'
    )

    garage = fields.Integer(
        string='Garage',
        help='Number of garage spaces'
    )

    parking = fields.Char(
        string='Parking',
        help='Parking information'
    )

    # Status
    status = fields.Selection(
        [
            ('new', 'New Listing'),
            ('rejected', 'Not Interested'),
            ('interested', 'Interested'),
        ],
        string='Status',
        default='interested',
        help='Your workflow status for this listing',
        tracking=True,
    )

    market_status = fields.Selection(
        [
            ('active', 'Active'),
            ('pending', 'Pending'),
            ('contingent', 'Contingent'),
            ('sold', 'Sold'),
            ('off_market', 'Off Market'),
        ],
        string='Market Status',
        help='Status reported by the listing source',
        tracking=True,
    )

    # new_construction removed; use is_new_construction

    # Dates
    listing_date = fields.Datetime(
        string='List Date',
        help='Date and time when the property was listed',
        tracking=True,
    )

    pending_date = fields.Datetime(
        string='Pending Date',
        help='Date and time when the property went pending',
        tracking=True,
    )

    sold_date = fields.Datetime(
        string='Sold Date',
        help='Date and time when the property was sold',
        tracking=True,
    )

    # Year and tax information
    year_built = fields.Integer(
        string='Year Built',
        help='Year the property was built',
        digits=(4, 0)
    )

    annual_tax = fields.Monetary(
        string='Annual Tax Value',
        currency_field='currency_id',
        related='last_tax_id.tax',
        store=True,
        readonly=True,
        help='Tax assessed value of the property (from latest tax record)',
        tracking=True,
    )

    assessed_value = fields.Monetary(
        string='Assessed Value',
        currency_field='currency_id',
        related='last_tax_id.assessment_total',
        store=True,
        readonly=True,
        help='Assessed value of the property (from latest tax record)',
        tracking=True,
    )

    tax_history_ids = fields.One2many(
        'real_estate.tax_history',
        'property_id',
        string='Tax History',
        help='History of property taxes'
    )

    # Tax Record Fields
    tax_record_apn = fields.Char(
        string='APN',
        help='Assessor Parcel Number'
    )

    tax_record_cl_id = fields.Char(
        string='CL ID',
        help='CoreLogic ID'
    )

    tax_record_last_update_date = fields.Datetime(
        string='Tax Record Last Update',
        help='Date when tax record was last updated'
    )

    tax_record_public_record_id = fields.Char(
        string='Public Record ID',
        help='Public record identifier'
    )

    tax_record_tax_parcel_id = fields.Char(
        string='Tax Parcel ID',
        help='Tax parcel identifier'
    )

    feature_ids = fields.One2many(
        'real_estate.feature',
        'property_id',
        string='Property Features',
        help='Detailed features of the property'
    )

    estimate_ids = fields.One2many(
        'real_estate.estimate',
        'property_id',
        string='Property Estimates',
        help='Historical property value estimates'
    )

    hoa_fee = fields.Monetary(
        string='HOA Fee',
        currency_field='currency_id',
        help='Homeowners Association fee'
    )

    monthly_fees = fields.Text(
        string='Monthly Fees',
        help='Monthly fees associated with the property'
    )

    one_time_fees = fields.Text(
        string='One-time Fees',
        help='One-time fees associated with the property'
    )

    # Location information
    address = fields.Char(
        string='Address',
        required=True,
        help='Full property address'
    )

    latitude = fields.Float(
        string='Latitude',
        digits=(16, 10),
        help='Latitude coordinate'
    )

    longitude = fields.Float(
        string='Longitude',
        digits=(16, 10),
        help='Longitude coordinate'
    )

    fips_code = fields.Char(
        string='FIPS Code',
        help='Federal Information Processing Standards code'
    )

    parcel_number = fields.Char(
        string='Parcel Number',
        help='Property parcel number'
    )

    # URL field
    url = fields.Char(
        string='URL',
        help='Link to the property listing online',
        tracking=True,
    )

    # Agent/Broker information
    agent_name = fields.Char(
        string='Agent Name',
        help='Name of the listing agent'
    )

    agent_phone = fields.Char(
        string='Agent Phone',
        help='Phone number of the listing agent'
    )

    agent_email = fields.Char(
        string='Agent Email',
        help='Email of the listing agent'
    )

    agent_uuid = fields.Char(
        string='Agent UUID',
        help='Unique identifier for the agent'
    )

    agent_state_license = fields.Char(
        string='Agent License',
        help='Agent state license number'
    )

    broker_name = fields.Char(
        string='Broker Name',
        help='Name of the broker'
    )

    broker_uuid = fields.Char(
        string='Broker UUID',
        help='Unique identifier for the broker'
    )

    office_name = fields.Char(
        string='Office Name',
        help='Name of the real estate office'
    )

    office_uuid = fields.Char(
        string='Office UUID',
        help='Unique identifier for the office'
    )

    office_email = fields.Char(
        string='Office Email',
        help='Email of the real estate office'
    )

    # MLS information
    days_on_mls = fields.Integer(
        string='Days on MLS',
        help='Number of days the property has been on MLS'
    )

    # Additional information
    estimated_monthly_rental = fields.Monetary(
        string='Est. Monthly Rental',
        currency_field='currency_id',
        help='Estimated monthly rental income'
    )

    # Property Flags
    is_coming_soon = fields.Boolean(
        string='Coming Soon',
        help='Property is coming soon',
        tracking=True,
    )

    is_contingent = fields.Boolean(
        string='Contingent',
        help='Property sale is contingent',
        tracking=True,
    )

    is_foreclosure = fields.Boolean(
        string='Foreclosure',
        help='Property is a foreclosure',
        tracking=True,
    )

    is_new_construction = fields.Boolean(
        string='New Construction',
        help='Property is new construction',
        tracking=True,
    )

    is_new_listing = fields.Boolean(
        string='New Listing',
        help='Property is a new listing',
        tracking=True,
    )

    is_pending = fields.Boolean(
        string='Pending',
        help='Property sale is pending',
        tracking=True,
    )

    is_price_reduced = fields.Boolean(
        string='Price Reduced',
        help='Property price has been reduced',
        tracking=True,
    )

    pet_policy = fields.Char(
        string='Pet Policy',
        help='Pet policy information'
    )

    terms = fields.Char(
        string='Terms',
        help='Listing terms'
    )

    # Nearby schools relation (replaces text field)
    nearby_school_ids = fields.Many2many(
        'real_estate.school',
        'real_estate_listing_school_rel',
        'listing_id',
        'school_id',
        string='Nearby Schools',
        help='Nearby schools related to this listing'
    )

    # Photos and media
    primary_photo = fields.Char(
        string='Primary Photo',
        related='primary_image_id.href',
        store=True,
        readonly=True,
        help='URL to the primary photo'
    )

    photos = fields.Text(
        string='Photos',
        help='URLs to property photos'
    )

    alt_photos = fields.Text(
        string='Alt Photos',
        help='URLs to alternative property photos'
    )

    # Relationship to photos model
    photo_ids = fields.One2many(
        'real_estate.photo',
        'property_id',
        string='Property Photos',
        help='Photos of the property'
    )

    # Relationship to popularity metrics
    popularity_ids = fields.One2many(
        'real_estate.popularity',
        'property_id',
        string='Popularity Metrics',
        help='Popularity metrics for different time periods'
    )

    # Open houses and units
    open_houses = fields.Text(
        string='Open Houses',
        help='Open house information'
    )

    units = fields.Text(
        string='Units',
        help='Information about units for multi-family properties'
    )

    # Estimates
    current_estimates = fields.Text(
        string='Current Estimates',
        help='Current property value estimates'
    )

    estimates = fields.Text(
        string='Historical Estimates',
        help='Historical property value estimates'
    )

    # Tags for quick tagging
    listing_tag_ids = fields.Many2many(
        'real_estate.tag',
        string='Tags',
        tracking=True,
    )

    # User tags field
    user_tag_ids = fields.Many2many(
        'real_estate.tag',
        'real_estate_user_tag_rel',
        'real_estate_id',
        'tag_id',
        string='Tags',
        help="Tags you assigned to this property",
        tracking=True,
    )

    property_tags = fields.Text(
        string='Property Tags',
        help='Property-level tags from the listing source (JSON format)'
    )

    user_notes = fields.Html(
        string='Notes & Thoughts',
        help='Your thoughts on this house',
    )

    user_pros = fields.Html(
        string='Things to love',
        help='Positive aspects of this property',
    )

    user_cons = fields.Html(
        string='Concerns & Questions',
        help='Negative aspects of this property',
    )

    is_favorite = fields.Boolean(
        string='Favorite',
        help='Whether the property is a favorite'
    )

    # Computed fields
    days_on_market = fields.Integer(
        string='Days on Market',
        compute='_compute_days_on_market',
        store=True
    )

    price_per_sqft = fields.Monetary(
        string='Price per Sqft',
        currency_field='currency_id',
        compute='_compute_price_per_sqft',
        store=True,
        help='Calculated price per square foot',
        tracking=True,
    )

    price_vs_last_sold = fields.Monetary(
        string='Price vs Last Sold',
        currency_field='currency_id',
        compute='_compute_price_differences',
        store=True,
        help='Difference between current list price and last sold price',
        tracking=True,
    )

    price_vs_estimate = fields.Monetary(
        string='Price vs Estimate',
        currency_field='currency_id',
        compute='_compute_price_differences',
        store=True,
        help='Difference between current list price and estimated value',
        tracking=True,
    )

    primary_image_id = fields.Many2one(
        'real_estate.photo',
        string='Primary Image',
        compute='_compute_primary_image_id',
        store=True,
        help='Primary image for the property'
    )

    # Calculated fields about the primary image id
    primary_image_id_preview_url = fields.Char(
        string='Primary Image URL',
        related='primary_image_id.href'
    )

    last_tax_id = fields.Many2one(
        'real_estate.tax_history',
        string='Latest Tax Record',
        compute='_compute_last_tax_id',
        store=True,
        help='Most recent tax history record'
    )

    best_estimate_id = fields.Many2one(
        'real_estate.estimate',
        string='Best Estimate',
        compute='_compute_best_estimate_id',
        store=True,
        help='Estimate marked as best home value'
    )

    years_since_sold = fields.Float(
        string='Years Since Sold',
        compute='_compute_years_since_sold',
        store=True,
        digits=(10, 2),
        help='Number of years since the property was last sold'
    )

    # Stat button count fields
    photo_count = fields.Integer(
        string='Photo Count',
        compute='_compute_counts',
        help='Number of photos for this property'
    )

    estimate_count = fields.Integer(
        string='Estimate Count',
        compute='_compute_counts',
        help='Number of estimates for this property'
    )

    tax_history_count = fields.Integer(
        string='Tax History Count',
        compute='_compute_counts',
        help='Number of tax history records for this property'
    )

    popularity_count = fields.Integer(
        string='Popularity Count',
        compute='_compute_counts',
        help='Number of popularity records for this property'
    )

    feature_count = fields.Integer(
        string='Feature Count',
        compute='_compute_counts',
        help='Number of feature records for this property'
    )

    popularity_saves_28_days = fields.Integer(
        string='Saves (28 days)',
        compute='_compute_popularity_saves',
        help='Number of saves in the last 28 days'
    )

    def write(self, vals):
        res = super().write(vals)

        try:
            bus = self.env["bus.bus"]
            payload_fields = list(vals.keys())

            for record in self:
                channel = f"estate_property_{record.id}"

                payload = {
                    "id": record.id,
                    "model": record._name,
                    "updated_fields": payload_fields,
                }

                _logger.info(
                    "[RealEstate.write] Sending bus notification | channel=%s type=%s payload=%s",
                    channel,
                    "estate_property_update",
                    payload,
                )

                # Use Odoo's low-level API with explicit notification type
                bus._sendone(channel, "estate_property_update", payload)
        except Exception as e:
            # Do not block writes if bus fails; just log.
            _logger.warning("Failed to send bus notification for RealEstate.write: %s", e)
        return res

    @api.depends('listing_date')
    def _compute_days_on_market(self):
        today = fields.Date.today()
        for record in self:
            if record.listing_date:
                # Convert datetime to date before subtraction
                listing_date = record.listing_date.date()
                delta = today - listing_date
                record.days_on_market = delta.days
            else:
                record.days_on_market = 0

    @api.depends('price', 'sqft')
    def _compute_price_per_sqft(self):
        for record in self:
            if record.price and record.sqft and record.sqft > 0:
                record.price_per_sqft = record.price / record.sqft
            else:
                record.price_per_sqft = 0

    @api.depends('lot_sqft')
    def _compute_lot_acres(self):
        for record in self:
            if record.lot_sqft and record.lot_sqft > 0:
                # Convert square feet to acres (1 acre = 43,560 sq ft)
                record.lot_acres = record.lot_sqft / 43560.0
            else:
                record.lot_acres = 0

    def _inverse_lot_acres(self):
        """When acres is set (e.g. via RPC/json2), update lot_sqft accordingly."""
        for record in self:
            acres = record.lot_acres or 0.0
            if acres > 0:
                record.lot_sqft = int(round(acres * 43560.0))
            else:
                record.lot_sqft = 0

    @api.depends('price', 'last_sold_price', 'estimated_value')
    def _compute_price_differences(self):
        for record in self:
            # Price vs Last Sold
            if record.price and record.last_sold_price:
                record.price_vs_last_sold = record.price - record.last_sold_price
            else:
                record.price_vs_last_sold = 0

            # Price vs Estimate
            if record.price and record.estimated_value:
                record.price_vs_estimate = record.price - record.estimated_value
            else:
                record.price_vs_estimate = 0

    @api.depends('photo_ids.is_primary', 'photo_ids.sequence')
    def _compute_primary_image_id(self):
        for record in self:
            # First try to find a photo marked as primary
            primary_photo = record.photo_ids.filtered('is_primary')
            if primary_photo:
                record.primary_image_id = primary_photo[0]
            elif record.photo_ids:
                # If no primary photo, get the first one by sequence
                record.primary_image_id = record.photo_ids.sorted('sequence')[0]
            else:
                record.primary_image_id = False

    @api.depends('tax_history_ids.year')
    def _compute_last_tax_id(self):
        for record in self:
            if record.tax_history_ids:
                # Get the tax record with the highest year
                latest_tax = record.tax_history_ids.sorted('year', reverse=True)
                record.last_tax_id = latest_tax[0] if latest_tax else False
            else:
                record.last_tax_id = False

    @api.depends('estimate_ids.is_best_home_value')
    def _compute_best_estimate_id(self):
        for record in self:
            # Find the estimate marked as best home value
            best_estimate = record.estimate_ids.filtered('is_best_home_value')
            record.best_estimate_id = best_estimate[0] if best_estimate else False

    @api.depends('sold_date')
    def _compute_years_since_sold(self):
        today = fields.Date.today()
        for record in self:
            if record.sold_date:
                # Convert datetime to date before calculation
                sold_date = record.sold_date.date()
                delta = today - sold_date
                # Convert days to years with 2 decimal precision
                record.years_since_sold = round(delta.days / 365.25, 2)
            else:
                record.years_since_sold = 0

    @api.depends('photo_ids', 'estimate_ids', 'tax_history_ids', 'popularity_ids', 'feature_ids')
    def _compute_counts(self):
        for record in self:
            record.photo_count = len(record.photo_ids)
            record.estimate_count = len(record.estimate_ids)
            record.tax_history_count = len(record.tax_history_ids)
            record.popularity_count = len(record.popularity_ids)
            record.feature_count = len(record.feature_ids)

    @api.depends('popularity_ids.last_n_days', 'popularity_ids.saves_total')
    def _compute_popularity_saves(self):
        for record in self:
            # Find popularity record for 28 days
            popularity_28 = record.popularity_ids.filtered(lambda p: p.last_n_days == 28)
            record.popularity_saves_28_days = popularity_28.saves_total if popularity_28 else 0

    @api.depends('bedrooms', 'baths_full', 'baths_half')
    def _compute_bed_bath_description(self):
        for record in self:
            record.bed_bath_description = f'{record.bedrooms}bed/{record.baths_full}{".5" if record.baths_half > 0 else ""}bath'

    # --- Lot size sync via compute/inverse (works for UI and json2 RPC) ---

    # --- Bathrooms compute/inverse logic ---
    @api.depends('baths_full', 'baths_half')
    def _compute_baths_total(self):
        for rec in self:
            full = int(rec.baths_full or 0)
            half = 1 if int(rec.baths_half or 0) > 0 else 0
            rec.baths_total = float(full) + (0.5 if half else 0.0)

    def _inverse_baths_total(self):
        for rec in self:
            total = float(rec.baths_total or 0.0)
            # snap to nearest 0.5 increment
            total = round(total * 2) / 2.0
            whole = int(total)
            has_half = 1 if (total - whole) >= 0.5 else 0
            rec.baths_full = whole
            rec.baths_half = has_half

    def action_open_url(self):
        """Open the URL in a new browser tab"""
        self.ensure_one()

        if self.permalink:
            return {
                'type': 'ir.actions.act_url',
                'url': self.permalink,
                'target': 'new',
            }

        if self.url:
            return {
                'type': 'ir.actions.act_url',
                'url': self.url,
                'target': 'new',
            }

        return False

    def action_scrape_property(self):
        """
        Publish a message to RabbitMQ to trigger property scraping
        """
        self.ensure_one()

        # Check if we have an address to scrape
        if not self.address:
            raise UserError("Property address is required for scraping.")

        _logger.info(f'Publishing scrape request for property: {self.address}')

        try:
            # Get RabbitMQ connection parameters from environment variables
            rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
            rabbitmq_port = int(os.environ.get('RABBITMQ_PORT', 5672))
            rabbitmq_user = os.environ.get('RABBITMQ_USER', 'guest')
            rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'guest')
            rabbitmq_exchange = os.environ.get('RABBITMQ_EXCHANGE', 'property_exchange')
            rabbitmq_routing_key = os.environ.get('RABBITMQ_ROUTING_KEY', 'property.scrape')

            # Prepare message payload
            message = {
                'location': self.address,
                'listing_type': 'for_sale',
                'record_id': self.id,
                'limit': 1
            }

            # If URL is provided, try to extract more information
            if self.url:
                try:
                    parsed_url = urlparse(self.url)
                    message['source_url'] = self.url
                except Exception as e:
                    _logger.warning(f"Could not parse URL: {e}")

            # Connect to RabbitMQ
            credentials = pika.PlainCredentials(
                rabbitmq_user,
                rabbitmq_pass
            )

            parameters = pika.ConnectionParameters(
                host=rabbitmq_host,
                port=rabbitmq_port,
                credentials=credentials
            )

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Declare exchange
            channel.exchange_declare(
                exchange=rabbitmq_exchange,
                exchange_type='topic',
                durable=True
            )

            # Publish message
            channel.basic_publish(
                exchange=rabbitmq_exchange,
                routing_key=rabbitmq_routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )

            _logger.info(f"Published message to RabbitMQ: {message}")

            # Close connection
            connection.close()

            # Show success message to user
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': f'Property scrape request sent for {self.address}',
                    'sticky': False,
                    'type': 'success',
                }
            }

        except Exception as e:
            _logger.error(f"Error publishing to RabbitMQ: {str(e)}")
            raise UserError(f"Failed to send scrape request: {str(e)}")

    @api.model
    def cron_scrape_active_properties(self):
        """
        Cronjob method to scrape all properties that are not 'off_market'
        This method is called daily by the scheduled action
        """
        _logger.info('Starting daily scrape of active properties')

        # Find all properties that are not off market and have an address
        active_properties = self.search([
            ('market_status', '!=', 'off_market'),
            ('address', '!=', False),
            ('address', '!=', '')
        ])

        _logger.info(f'Found {len(active_properties)} active properties to scrape')

        success_count = 0
        error_count = 0

        for property_record in active_properties:
            try:
                property_record.action_scrape_property()
                success_count += 1
                _logger.info(f'Successfully queued scrape for property: {property_record.address}')
            except Exception as e:
                error_count += 1
                _logger.error(f'Failed to queue scrape for property {property_record.address}: {str(e)}')

        _logger.info(f'Daily scrape completed. Success: {success_count}, Errors: {error_count}')

        return {
            'success_count': success_count,
            'error_count': error_count,
            'total_properties': len(active_properties)
        }

    def action_ask_chatgpt(self):
        pass

    def action_view_photos(self):
        """Open photos in kanban view"""
        self.ensure_one()

        return {
            'name': f'Photos - {self.address}',
            'type': 'ir.actions.act_window',
            'res_model': 'real_estate.photo',
            'view_mode': 'kanban,list,form',
            'domain': [('property_id', '=', self.id)],
            'context': {'default_property_id': self.id},
        }

    def action_view_estimates(self):
        """Open estimates in list view"""
        self.ensure_one()

        return {
            'name': f'Estimates - {self.address}',
            'type': 'ir.actions.act_window',
            'res_model': 'real_estate.estimate',
            'view_mode': 'list,form',
            'domain': [('property_id', '=', self.id)],
            'context': {'default_property_id': self.id},
        }

    def action_view_tax_history(self):
        """Open tax history in list view"""
        self.ensure_one()

        return {
            'name': f'Tax History - {self.address}',
            'type': 'ir.actions.act_window',
            'res_model': 'real_estate.tax_history',
            'view_mode': 'list,form',
            'domain': [('property_id', '=', self.id)],
            'context': {'default_property_id': self.id},
        }

    def action_view_popularity(self):
        """Open popularity in chart view"""
        self.ensure_one()

        return {
            'name': f'Popularity - {self.address}',
            'type': 'ir.actions.act_window',
            'res_model': 'real_estate.popularity',
            'view_mode': 'list,graph,form',
            'domain': [('property_id', '=', self.id)],
            'context': {'default_property_id': self.id},
        }

    def action_view_features(self):
        """Open features in list view, grouped by parent category and category"""
        self.ensure_one()

        return {
            'name': f'Features - {self.address}',
            'type': 'ir.actions.act_window',
            'res_model': 'real_estate.feature',
            'view_mode': 'list,form',
            'domain': [('property_id', '=', self.id)],
            'context': {
                'default_property_id': self.id,
                'group_by': ['parent_category', 'category'],
            },
        }
